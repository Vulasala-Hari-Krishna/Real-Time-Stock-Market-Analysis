"""Unit tests for the Snowflake snapshot loader (src/load/snowflake_snapshot.py)."""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.export.gold_snapshot import (
    Manifest,
    ManifestColumn,
    ManifestDataset,
    ManifestFile,
    SourceTableRef,
)
from src.load import snowflake_snapshot as loader


# ---------------------------------------------------------------------------
# Fakes: a minimal DB-API-shaped connection/cursor that records every
# executed statement/params so SQL-building logic can be asserted directly,
# instead of mocking the functions that build that SQL.
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, fetchone_results: list[Any] | None = None) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self._fetchone_results = list(fetchone_results or [])
        self.closed = False

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Any:
        return self._fetchone_results.pop(0) if self._fetchone_results else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.autocommit_calls: list[bool] = []
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        pass


@pytest.fixture()
def config() -> loader.SnowflakeLoadConfig:
    return loader.SnowflakeLoadConfig(
        account="ILMRWBU-TX52777",
        user="LOADER_USER",
        role="STOCK_MARKET_DEV_LOADER",
        warehouse="STOCK_MARKET_DEV_WH",
        database="STOCK_MARKET_DEV",
        staging_schema="PUBLISH_STAGING",
        serving_schema="SERVING",
        stage_name="STOCK_MARKET_DEV.PUBLISH_STAGING.STOCK_MARKET_DEV_PUBLISH_STAGE",
        bucket="offline-test-bucket",
        dataset="daily_quote_summary",
    )


def _manifest_entry(
    row_count: int = 2, batch_id: str = "batch-1"
) -> tuple[Manifest, ManifestDataset]:
    columns = [
        ManifestColumn(name=name, type="string", nullable=True)
        for name, _ in loader.DATASET_COLUMNS["daily_quote_summary"]
    ]
    dataset = ManifestDataset(
        name="daily_quote_summary",
        row_count=row_count,
        business_keys=["provider", "symbol", "capture_date_utc"],
        columns=columns,
        files=[
            ManifestFile(
                key=f"publish/batches/{batch_id}/daily_quote_summary/part-00000.parquet",
                size_bytes=123,
                sha256="a" * 64,
                row_count=row_count,
            )
        ],
    )
    manifest = Manifest(
        batch_id=batch_id,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        producer_run_id="run-1",
        logical_cutoff="2026-01-01",
        publication_sequence=["daily_quote_summary"],
        source_tables={
            "daily_quote_summary": SourceTableRef(
                table="stock_market_dev.stocks_gold.daily_quote_summary",
                source_path="s3://bucket/lakehouse/gold/stock_market_dev/stocks/daily_quote_summary",
                version=5,
            )
        },
        datasets=[dataset],
    )
    return manifest, dataset


# ---------------------------------------------------------------------------
# SnowflakeLoadConfig
# ---------------------------------------------------------------------------
def test_config_qualified(config: loader.SnowflakeLoadConfig) -> None:
    assert (
        config.qualified("SERVING", "DAILY_QUOTE_SUMMARY")
        == "STOCK_MARKET_DEV.SERVING.DAILY_QUOTE_SUMMARY"
    )


# ---------------------------------------------------------------------------
# _private_key_der (real cryptography, no mocks - must not regress against
# an actual RSA key the way the connector will really receive it)
# ---------------------------------------------------------------------------
def test_private_key_der_roundtrips_a_real_key() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    der = loader._private_key_der(pem, None)
    reloaded = serialization.load_der_private_key(der, password=None)
    assert reloaded.key_size == 2048


def test_private_key_der_supports_a_passphrase() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(b"secret"),
    ).decode()

    der = loader._private_key_der(pem, "secret")
    serialization.load_der_private_key(der, password=None)


# ---------------------------------------------------------------------------
# find_latest_batch_id
# ---------------------------------------------------------------------------
@patch("src.load.snowflake_snapshot.get_s3_client")
def test_find_latest_batch_id_skips_batch_without_manifest(
    mock_get_client: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "publish/batches/20260101T000000Z-aaaa/"},
                {"Prefix": "publish/batches/20260102T000000Z-bbbb/"},
            ]
        }
    ]

    def fake_head_object(Bucket: str, Key: str) -> dict:
        if "20260102" in Key:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    mock_client.head_object.side_effect = fake_head_object
    mock_get_client.return_value = mock_client

    assert loader.find_latest_batch_id("offline-test-bucket") == "20260101T000000Z-aaaa"


@patch("src.load.snowflake_snapshot.get_s3_client")
def test_find_latest_batch_id_returns_none_when_nothing_published(
    mock_get_client: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = [{}]
    mock_get_client.return_value = mock_client

    assert loader.find_latest_batch_id("offline-test-bucket") is None


# ---------------------------------------------------------------------------
# load_manifest
# ---------------------------------------------------------------------------
@patch("src.load.snowflake_snapshot.get_s3_client")
def test_load_manifest_rejects_non_completed_status(mock_get_client: MagicMock) -> None:
    manifest, _ = _manifest_entry()
    body = manifest.model_dump_json()
    body = body.replace('"completed"', '"in_progress"', 1)
    mock_client = MagicMock()
    mock_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=body.encode()))
    }
    mock_get_client.return_value = mock_client

    with pytest.raises(ValueError, match="not completed"):
        loader.load_manifest("offline-test-bucket", "batch-1")


# ---------------------------------------------------------------------------
# validate_expected_schema
# ---------------------------------------------------------------------------
def test_validate_expected_schema_passes_for_registered_dataset() -> None:
    _, entry = _manifest_entry()
    loader.validate_expected_schema(entry, "daily_quote_summary")


def test_validate_expected_schema_includes_bronze_version_lineage_column() -> None:
    """Regression test: a real live manifest (2026-09-26) included
    bronze_version - appended after summarize_quotes() by
    databricks_ticks.py::rebuild_outputs - and this loader's registry didn't
    account for it, rejecting every real batch as schema drift. Guards
    against silently dropping this column from DATASET_COLUMNS again."""
    assert ("bronze_version", "NUMBER(38,0)") in loader.DATASET_COLUMNS[
        "daily_quote_summary"
    ]


def test_validate_expected_schema_rejects_column_drift() -> None:
    _, entry = _manifest_entry()
    entry.columns = entry.columns[:-1]  # drop one expected column
    with pytest.raises(ValueError, match="Schema drift"):
        loader.validate_expected_schema(entry, "daily_quote_summary")


def test_validate_expected_schema_rejects_business_key_drift() -> None:
    _, entry = _manifest_entry()
    entry.business_keys = ["provider", "symbol"]
    with pytest.raises(ValueError, match="Business-key drift"):
        loader.validate_expected_schema(entry, "daily_quote_summary")


# ---------------------------------------------------------------------------
# copy_into_staging
# ---------------------------------------------------------------------------
def test_copy_into_staging_builds_delete_then_copy_with_exact_files(
    config: loader.SnowflakeLoadConfig,
) -> None:
    _, entry = _manifest_entry(batch_id="batch-1")
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    loader.copy_into_staging(conn, config, entry, "batch-1")

    assert len(cursor.executed) == 2
    delete_sql, _ = cursor.executed[0]
    copy_sql, _ = cursor.executed[1]
    assert (
        delete_sql
        == "DELETE FROM STOCK_MARKET_DEV.PUBLISH_STAGING.DAILY_QUOTE_SUMMARY_STAGING"
    )
    assert (
        "COPY INTO STOCK_MARKET_DEV.PUBLISH_STAGING.DAILY_QUOTE_SUMMARY_STAGING"
        in copy_sql
    )
    assert "$1:provider::VARCHAR" in copy_sql
    assert "$1:capture_date_utc::DATE" in copy_sql
    assert "FROM (SELECT" in copy_sql and f"FROM @{config.stage_name})" in copy_sql
    assert (
        "FILES = ('batches/batch-1/daily_quote_summary/part-00000.parquet')" in copy_sql
    )
    assert "FILE_FORMAT = (TYPE = PARQUET)" in copy_sql


def test_copy_into_staging_rejects_file_outside_batch_prefix(
    config: loader.SnowflakeLoadConfig,
) -> None:
    _, entry = _manifest_entry(batch_id="batch-1")
    entry.files[0].key = (
        "publish/batches/OTHER-BATCH/daily_quote_summary/part-00000.parquet"
    )
    conn = FakeConnection(FakeCursor())

    with pytest.raises(ValueError, match="outside batch prefix"):
        loader.copy_into_staging(conn, config, entry, "batch-1")


# ---------------------------------------------------------------------------
# validate_staging
# ---------------------------------------------------------------------------
def test_validate_staging_passes_and_returns_row_count(
    config: loader.SnowflakeLoadConfig,
) -> None:
    _, entry = _manifest_entry(row_count=2)
    cursor = FakeCursor(fetchone_results=[(2,), None])
    conn = FakeConnection(cursor)

    assert loader.validate_staging(conn, config, entry) == 2


def test_validate_staging_rejects_row_count_mismatch(
    config: loader.SnowflakeLoadConfig,
) -> None:
    _, entry = _manifest_entry(row_count=2)
    cursor = FakeCursor(fetchone_results=[(1,)])
    conn = FakeConnection(cursor)

    with pytest.raises(ValueError, match="row count mismatch"):
        loader.validate_staging(conn, config, entry)


def test_validate_staging_rejects_duplicate_business_keys(
    config: loader.SnowflakeLoadConfig,
) -> None:
    _, entry = _manifest_entry(row_count=2)
    cursor = FakeCursor(
        fetchone_results=[(2,), ("alpha_vantage", "AAPL", "2026-01-01", 2)]
    )
    conn = FakeConnection(cursor)

    with pytest.raises(ValueError, match="Duplicate business keys"):
        loader.validate_staging(conn, config, entry)


# ---------------------------------------------------------------------------
# publish_serving
# ---------------------------------------------------------------------------
def test_publish_serving_commits_delete_then_insert(
    config: loader.SnowflakeLoadConfig,
) -> None:
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    loader.publish_serving(conn, config, "batch-1")

    assert conn.autocommit_calls == [False, True]
    assert conn.committed is True
    assert conn.rolled_back is False
    delete_sql, _ = cursor.executed[0]
    insert_sql, insert_params = cursor.executed[1]
    assert delete_sql == "DELETE FROM STOCK_MARKET_DEV.SERVING.DAILY_QUOTE_SUMMARY"
    assert insert_sql.startswith(
        "INSERT INTO STOCK_MARKET_DEV.SERVING.DAILY_QUOTE_SUMMARY"
    )
    assert insert_params == ("batch-1",)


def test_publish_serving_rolls_back_on_failure(
    config: loader.SnowflakeLoadConfig,
) -> None:
    class FailingCursor(FakeCursor):
        def execute(self, sql: str, params: tuple = ()) -> None:
            super().execute(sql, params)
            if sql.startswith("INSERT"):
                raise RuntimeError("boom")

    conn = FakeConnection(FailingCursor())

    with pytest.raises(RuntimeError, match="boom"):
        loader.publish_serving(conn, config, "batch-1")

    assert conn.rolled_back is True
    assert conn.committed is False
    assert conn.autocommit_calls == [False, True]


# ---------------------------------------------------------------------------
# run_load (orchestration - mocks the S3/Snowflake boundary functions)
# ---------------------------------------------------------------------------
def test_run_load_rejects_unregistered_dataset(
    config: loader.SnowflakeLoadConfig,
) -> None:
    bad_config = config.model_copy(update={"dataset": "not_a_real_dataset"})
    with pytest.raises(ValueError, match="Unregistered dataset"):
        loader.run_load(bad_config, private_key_pem="pem")


@patch("src.load.snowflake_snapshot.connect")
@patch("src.load.snowflake_snapshot.verify_published_batch")
@patch("src.load.snowflake_snapshot.load_manifest")
@patch("src.load.snowflake_snapshot.find_latest_batch_id")
def test_run_load_skips_batch_already_completed(
    mock_find: MagicMock,
    mock_load_manifest: MagicMock,
    mock_verify: MagicMock,
    mock_connect: MagicMock,
    config: loader.SnowflakeLoadConfig,
) -> None:
    manifest, _ = _manifest_entry(batch_id="batch-1")
    mock_find.return_value = "batch-1"
    mock_load_manifest.return_value = manifest
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    with patch("src.load.snowflake_snapshot.ensure_objects_exist"), patch(
        "src.load.snowflake_snapshot.get_ledger_row",
        return_value={"status": "completed", "source_version": 5},
    ), patch("src.load.snowflake_snapshot.copy_into_staging") as mock_copy:
        result = loader.run_load(config, private_key_pem="pem")

    assert result.status == "skipped_already_loaded"
    assert result.rows_loaded == 0
    mock_copy.assert_not_called()
    mock_conn.close.assert_called_once()


@patch("src.load.snowflake_snapshot.connect")
@patch("src.load.snowflake_snapshot.verify_published_batch")
@patch("src.load.snowflake_snapshot.load_manifest")
@patch("src.load.snowflake_snapshot.find_latest_batch_id")
def test_run_load_rejects_stale_source_version(
    mock_find: MagicMock,
    mock_load_manifest: MagicMock,
    mock_verify: MagicMock,
    mock_connect: MagicMock,
    config: loader.SnowflakeLoadConfig,
) -> None:
    manifest, _ = _manifest_entry(batch_id="batch-2")
    mock_find.return_value = "batch-2"
    mock_load_manifest.return_value = manifest
    mock_connect.return_value = MagicMock()

    with patch("src.load.snowflake_snapshot.ensure_objects_exist"), patch(
        "src.load.snowflake_snapshot.get_ledger_row", return_value=None
    ), patch(
        "src.load.snowflake_snapshot.get_latest_completed_version", return_value=9
    ):
        with pytest.raises(ValueError, match="Stale or duplicate batch"):
            loader.run_load(config, private_key_pem="pem")


@patch("src.load.snowflake_snapshot.connect")
@patch("src.load.snowflake_snapshot.verify_published_batch")
@patch("src.load.snowflake_snapshot.load_manifest")
@patch("src.load.snowflake_snapshot.find_latest_batch_id")
def test_run_load_full_happy_path_publishes_and_completes(
    mock_find: MagicMock,
    mock_load_manifest: MagicMock,
    mock_verify: MagicMock,
    mock_connect: MagicMock,
    config: loader.SnowflakeLoadConfig,
) -> None:
    manifest, _ = _manifest_entry(batch_id="batch-2", row_count=3)
    mock_find.return_value = "batch-2"
    mock_load_manifest.return_value = manifest
    mock_connect.return_value = MagicMock()

    call_order: list[str] = []
    with patch("src.load.snowflake_snapshot.ensure_objects_exist"), patch(
        "src.load.snowflake_snapshot.get_ledger_row", return_value=None
    ), patch(
        "src.load.snowflake_snapshot.get_latest_completed_version", return_value=None
    ), patch(
        "src.load.snowflake_snapshot.start_batch",
        side_effect=lambda *a, **kw: call_order.append("start_batch"),
    ), patch(
        "src.load.snowflake_snapshot.copy_into_staging",
        side_effect=lambda *a, **kw: call_order.append("copy_into_staging"),
    ), patch(
        "src.load.snowflake_snapshot.validate_staging",
        side_effect=lambda *a, **kw: call_order.append("validate_staging") or 3,
    ), patch(
        "src.load.snowflake_snapshot.publish_serving",
        side_effect=lambda *a, **kw: call_order.append("publish_serving"),
    ), patch(
        "src.load.snowflake_snapshot.complete_batch",
        side_effect=lambda *a, **kw: call_order.append("complete_batch"),
    ) as mock_complete, patch(
        "src.load.snowflake_snapshot.fail_batch"
    ) as mock_fail:
        result = loader.run_load(config, private_key_pem="pem")

    assert call_order == [
        "start_batch",
        "copy_into_staging",
        "validate_staging",
        "publish_serving",
        "complete_batch",
    ]
    mock_fail.assert_not_called()
    mock_complete.assert_called_once()
    assert result.status == "completed"
    assert result.batch_id == "batch-2"
    assert result.rows_loaded == 3


@patch("src.load.snowflake_snapshot.connect")
@patch("src.load.snowflake_snapshot.verify_published_batch")
@patch("src.load.snowflake_snapshot.load_manifest")
@patch("src.load.snowflake_snapshot.find_latest_batch_id")
def test_run_load_marks_ledger_failed_when_copy_raises(
    mock_find: MagicMock,
    mock_load_manifest: MagicMock,
    mock_verify: MagicMock,
    mock_connect: MagicMock,
    config: loader.SnowflakeLoadConfig,
) -> None:
    manifest, _ = _manifest_entry(batch_id="batch-3")
    mock_find.return_value = "batch-3"
    mock_load_manifest.return_value = manifest
    mock_connect.return_value = MagicMock()

    with patch("src.load.snowflake_snapshot.ensure_objects_exist"), patch(
        "src.load.snowflake_snapshot.get_ledger_row", return_value=None
    ), patch(
        "src.load.snowflake_snapshot.get_latest_completed_version", return_value=None
    ), patch(
        "src.load.snowflake_snapshot.start_batch"
    ), patch(
        "src.load.snowflake_snapshot.copy_into_staging",
        side_effect=RuntimeError("copy exploded"),
    ), patch(
        "src.load.snowflake_snapshot.publish_serving"
    ) as mock_publish, patch(
        "src.load.snowflake_snapshot.fail_batch"
    ) as mock_fail:
        with pytest.raises(RuntimeError, match="copy exploded"):
            loader.run_load(config, private_key_pem="pem")

    mock_publish.assert_not_called()
    mock_fail.assert_called_once()
    assert mock_fail.call_args.args[2] == "batch-3"
