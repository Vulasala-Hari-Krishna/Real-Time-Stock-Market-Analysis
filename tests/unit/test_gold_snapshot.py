"""Unit tests for the gold snapshot exporter (src/export/gold_snapshot.py)."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from src.export import gold_snapshot as export


@pytest.fixture()
def config() -> export.GoldExportConfig:
    return export.GoldExportConfig(
        catalog="portfolio_dev",
        schema_prefix="stocks",
        bucket="offline-test-bucket",
        dataset="daily_quote_summary",
    )


def summary_table(rows: list[tuple[str, str, date, float]]) -> pa.Table:
    """Build a minimal daily_quote_summary-shaped table for testing."""
    return pa.table(
        {
            "provider": [r[0] for r in rows],
            "symbol": [r[1] for r in rows],
            "capture_date_utc": [r[2] for r in rows],
            "last_observed_price": [r[3] for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# GoldExportConfig
# ---------------------------------------------------------------------------
def test_config_paths(config: export.GoldExportConfig) -> None:
    assert (
        config.source_path()
        == "s3://offline-test-bucket/lakehouse/gold/portfolio_dev/stocks/daily_quote_summary"
    )
    assert config.table_identifier() == "portfolio_dev.stocks_gold.daily_quote_summary"
    assert (
        config.publish_prefix("batch-1")
        == "publish/batches/batch-1/daily_quote_summary"
    )


# ---------------------------------------------------------------------------
# _read_delta_rows (real deltalake + DuckDB, no mocks - this is what caught
# the deletionVectors incompatibility live and must not regress)
# ---------------------------------------------------------------------------
def test_read_delta_rows_handles_deletion_vector_table(tmp_path: Path) -> None:
    from deltalake import write_deltalake

    path = str(tmp_path / "dv_table").replace("\\", "/")
    write_deltalake(
        path,
        pa.table({"symbol": ["AAPL"], "price": [150.0]}),
        mode="overwrite",
        configuration={"delta.enableDeletionVectors": "true"},
    )
    write_deltalake(
        path, pa.table({"symbol": ["MSFT"], "price": [300.0]}), mode="overwrite"
    )

    latest = export._read_delta_rows(path, version=1, region=None)
    assert latest.to_pydict() == {"symbol": ["MSFT"], "price": [300.0]}

    pinned = export._read_delta_rows(path, version=0, region=None)
    assert pinned.to_pydict() == {"symbol": ["AAPL"], "price": [150.0]}


# ---------------------------------------------------------------------------
# assert_business_keys_unique
# ---------------------------------------------------------------------------
def test_unique_business_keys_pass() -> None:
    table = summary_table(
        [
            ("alpha_vantage", "AAPL", date(2026, 1, 1), 150.0),
            ("alpha_vantage", "MSFT", date(2026, 1, 1), 300.0),
        ]
    )
    export.assert_business_keys_unique(
        table, ("provider", "symbol", "capture_date_utc")
    )


def test_duplicate_business_keys_raise() -> None:
    table = summary_table(
        [
            ("alpha_vantage", "AAPL", date(2026, 1, 1), 150.0),
            ("alpha_vantage", "AAPL", date(2026, 1, 1), 151.0),
        ]
    )
    with pytest.raises(ValueError, match="Duplicate business keys"):
        export.assert_business_keys_unique(
            table, ("provider", "symbol", "capture_date_utc")
        )


def test_empty_table_has_no_duplicates() -> None:
    table = summary_table([])
    export.assert_business_keys_unique(
        table, ("provider", "symbol", "capture_date_utc")
    )


# ---------------------------------------------------------------------------
# export_dataset
# ---------------------------------------------------------------------------
@patch("src.export.gold_snapshot.get_s3_client")
def test_export_dataset_writes_one_part_and_matching_manifest_entry(
    mock_get_client: MagicMock, config: export.GoldExportConfig
) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    table = summary_table([("alpha_vantage", "AAPL", date(2026, 1, 1), 150.0)])

    entry = export.export_dataset(config, table, batch_id="batch-1")

    mock_client.put_object.assert_called_once()
    call_kwargs = mock_client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "offline-test-bucket"
    assert (
        call_kwargs["Key"]
        == "publish/batches/batch-1/daily_quote_summary/part-00000.parquet"
    )
    assert entry.row_count == 1
    assert entry.files[0].size_bytes == len(call_kwargs["Body"])
    assert {c.name for c in entry.columns} == {
        "provider",
        "symbol",
        "capture_date_utc",
        "last_observed_price",
    }
    assert entry.business_keys == ["provider", "symbol", "capture_date_utc"]


# ---------------------------------------------------------------------------
# verify_published_batch
# ---------------------------------------------------------------------------
def _manifest_for_one_file(key: str, body: bytes, batch_id: str) -> export.Manifest:
    import hashlib

    from src.export.gold_snapshot import (
        Manifest,
        ManifestColumn,
        ManifestDataset,
        ManifestFile,
        SourceTableRef,
    )

    return Manifest(
        batch_id=batch_id,
        created_at="2026-01-01T00:00:00+00:00",
        producer_run_id="run-1",
        logical_cutoff="2026-01-01",
        publication_sequence=["daily_quote_summary"],
        source_tables={
            "daily_quote_summary": SourceTableRef(
                table="c.s_gold.daily_quote_summary",
                source_path="s3://bucket/lakehouse/gold/c/s/daily_quote_summary",
                version=1,
            )
        },
        datasets=[
            ManifestDataset(
                name="daily_quote_summary",
                row_count=1,
                business_keys=["provider", "symbol", "capture_date_utc"],
                columns=[ManifestColumn(name="provider", type="string", nullable=True)],
                files=[
                    ManifestFile(
                        key=key,
                        size_bytes=len(body),
                        sha256=hashlib.sha256(body).hexdigest(),
                        row_count=1,
                    )
                ],
            )
        ],
    )


@patch("src.export.gold_snapshot.get_s3_client")
def test_verify_published_batch_passes_when_consistent(
    mock_get_client: MagicMock,
) -> None:
    body = b"parquet-bytes"
    key = "publish/batches/batch-1/daily_quote_summary/part-00000.parquet"
    manifest = _manifest_for_one_file(key, body, "batch-1")
    mock_client = MagicMock()
    mock_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=body))
    }
    mock_get_client.return_value = mock_client

    export.verify_published_batch("offline-test-bucket", manifest)


@patch("src.export.gold_snapshot.get_s3_client")
def test_verify_published_batch_rejects_checksum_mismatch(
    mock_get_client: MagicMock,
) -> None:
    key = "publish/batches/batch-1/daily_quote_summary/part-00000.parquet"
    manifest = _manifest_for_one_file(key, b"original-bytes", "batch-1")
    mock_client = MagicMock()
    mock_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b"tampered-bytes"))
    }
    mock_get_client.return_value = mock_client

    with pytest.raises(ValueError, match="Checksum mismatch"):
        export.verify_published_batch("offline-test-bucket", manifest)


def test_verify_published_batch_rejects_key_outside_batch_prefix() -> None:
    body = b"parquet-bytes"
    manifest = _manifest_for_one_file(
        "publish/batches/OTHER-BATCH/daily_quote_summary/part-00000.parquet",
        body,
        "batch-1",
    )
    with pytest.raises(ValueError, match="outside batch prefix"):
        export.verify_published_batch("offline-test-bucket", manifest)


# ---------------------------------------------------------------------------
# run_export
# ---------------------------------------------------------------------------
@patch("src.export.gold_snapshot.upload_json_to_s3")
@patch("src.export.gold_snapshot.get_s3_client")
@patch("src.export.gold_snapshot.read_gold_table")
def test_run_export_publishes_files_then_manifest_last(
    mock_read: MagicMock,
    mock_get_client: MagicMock,
    mock_upload_json: MagicMock,
    config: export.GoldExportConfig,
) -> None:
    table = summary_table([("alpha_vantage", "AAPL", date(2026, 1, 1), 150.0)])
    mock_read.return_value = (table, 7)

    call_order: list[str] = []
    published: dict[str, bytes] = {}

    def fake_put_object(**kwargs: object) -> None:
        published[kwargs["Key"]] = kwargs["Body"]  # type: ignore[index]
        call_order.append("put_object")

    def fake_get_object(**kwargs: object) -> dict:
        call_order.append("get_object")
        return {"Body": MagicMock(read=MagicMock(return_value=published[kwargs["Key"]]))}  # type: ignore[index]

    mock_client = MagicMock()
    mock_client.put_object.side_effect = fake_put_object
    mock_client.get_object.side_effect = fake_get_object
    mock_get_client.return_value = mock_client

    def fake_upload_json(data: dict, bucket: str, key: str) -> bool:
        call_order.append("upload_json")
        assert key.endswith("manifest.json")
        assert data["status"] == "completed"
        return True

    mock_upload_json.side_effect = fake_upload_json

    manifest = export.run_export(config, producer_run_id="run-123", batch_id="batch-1")

    assert call_order == ["put_object", "get_object", "upload_json"]
    assert manifest.batch_id == "batch-1"
    assert manifest.producer_run_id == "run-123"
    assert manifest.source_tables["daily_quote_summary"].version == 7
    assert manifest.datasets[0].row_count == 1
    assert manifest.logical_cutoff == "2026-01-01"


@patch("src.export.gold_snapshot.read_gold_table")
def test_run_export_rejects_empty_snapshot_by_default(
    mock_read: MagicMock, config: export.GoldExportConfig
) -> None:
    mock_read.return_value = (summary_table([]), 3)

    with pytest.raises(ValueError, match="Refusing an empty"):
        export.run_export(config, producer_run_id="run-123")


@patch("src.export.gold_snapshot.upload_json_to_s3", return_value=True)
@patch("src.export.gold_snapshot.get_s3_client")
@patch("src.export.gold_snapshot.read_gold_table")
def test_run_export_allows_explicit_empty_snapshot(
    mock_read: MagicMock,
    mock_get_client: MagicMock,
    mock_upload_json: MagicMock,
    config: export.GoldExportConfig,
) -> None:
    mock_read.return_value = (summary_table([]), 3)
    published: dict[str, bytes] = {}
    mock_client = MagicMock()
    mock_client.put_object.side_effect = lambda **kw: published.update(
        {kw["Key"]: kw["Body"]}
    )
    mock_client.get_object.side_effect = lambda **kw: {
        "Body": MagicMock(read=MagicMock(return_value=published[kw["Key"]]))
    }
    mock_get_client.return_value = mock_client

    manifest = export.run_export(
        config, producer_run_id="run-123", batch_id="batch-empty", allow_empty=True
    )

    assert manifest.datasets[0].row_count == 0
    assert manifest.logical_cutoff == "empty"


def test_run_export_rejects_unregistered_dataset() -> None:
    bad_config = export.GoldExportConfig(
        catalog="portfolio_dev",
        schema_prefix="stocks",
        bucket="offline-test-bucket",
        dataset="not_a_real_dataset",
    )
    with pytest.raises(ValueError, match="Unregistered dataset"):
        export.run_export(bad_config, producer_run_id="run-123")
