"""Gold snapshot exporter: publishes an immutable Parquet + manifest snapshot
of a Databricks gold Delta table for Snowflake to load, per the "Gold Snapshot
Contract v1" in docs/hybrid-migration.md.

Reads via the lightweight ``deltalake`` package (no Spark/Databricks compute
needed - the same approach the Streamlit dashboard already uses), so this
runs anywhere with S3 access: locally, in CI, or later from Airflow.

Contract highlights this module enforces:
  - Read at one pinned Delta version; never scan physical Parquet directly.
  - Write data files first, then ``manifest.json`` last - the manifest's
    existence is what makes a batch complete; readers must ignore a
    ``batch_id`` prefix with data files but no manifest.
  - Business keys must be unique in the exported snapshot.
  - A zero-row snapshot is only valid if explicitly requested.
"""

import argparse
import hashlib
import io
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from src.common.s3_utils import get_s3_client, upload_json_to_s3

logger = logging.getLogger(__name__)

MANIFEST_VERSION = 1

# Per-dataset business key (uniqueness) and cutoff-column (traceability)
# registry. Extend when a new dataset is added to this exporter; never guess
# a dataset's keys implicitly from its column list.
DATASET_BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "daily_quote_summary": ("provider", "symbol", "capture_date_utc"),
}
DATASET_CUTOFF_COLUMN: dict[str, str] = {
    "daily_quote_summary": "capture_date_utc",
}


class GoldExportConfig(BaseModel):
    """Identifies the exact gold table this exporter reads.

    Attributes:
        catalog: Existing Unity Catalog catalog owning the gold table.
        schema_prefix: Prefix of the gold schema (matches the Databricks job).
        bucket: Existing S3 bucket holding both lakehouse and publish prefixes.
        dataset: Registered dataset name; must exist in DATASET_BUSINESS_KEYS.
    """

    catalog: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_prefix: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    dataset: str

    def source_path(self) -> str:
        """Return the gold table's external Delta location."""
        return f"s3://{self.bucket}/lakehouse/gold/{self.catalog}/{self.schema_prefix}/{self.dataset}"

    def table_identifier(self) -> str:
        """Return the dataset's Unity Catalog identifier, for traceability only."""
        return f"{self.catalog}.{self.schema_prefix}_gold.{self.dataset}"

    def publish_prefix(self, batch_id: str) -> str:
        """Return this batch's publish prefix for the dataset (no trailing slash)."""
        return f"publish/batches/{batch_id}/{self.dataset}"


class ManifestFile(BaseModel):
    key: str
    size_bytes: int
    sha256: str
    row_count: int


class ManifestColumn(BaseModel):
    name: str
    type: str
    nullable: bool


class ManifestDataset(BaseModel):
    name: str
    row_count: int
    business_keys: list[str]
    columns: list[ManifestColumn]
    files: list[ManifestFile]


class SourceTableRef(BaseModel):
    table: str
    source_path: str
    version: int


class Manifest(BaseModel):
    manifest_version: int = MANIFEST_VERSION
    status: str = "completed"
    batch_id: str
    created_at: datetime
    producer_run_id: str
    logical_cutoff: str
    publication_sequence: list[str]
    source_tables: dict[str, SourceTableRef]
    datasets: list[ManifestDataset]


def _new_batch_id() -> str:
    """Generate a batch ID that sorts chronologically and is collision-safe."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid4().hex[:8]}"


def _s3_storage_options() -> dict[str, str] | None:
    """Build explicit S3 storage options from standard AWS env vars, if set.

    deltalake's Rust object_store backend generally picks up these same env
    vars on its own, but passing them explicitly (matching the dashboard's
    existing ``_s3_storage_options()``) avoids depending on that implicit
    pickup working identically across deltalake versions/environments -
    notably a GitHub Actions OIDC session, which sets AWS_SESSION_TOKEN
    alongside the access key.
    """
    options = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN"),
        "AWS_REGION": os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION"),
    }
    present = {k: v for k, v in options.items() if v}
    return present or None


def read_gold_table(
    config: GoldExportConfig, version: int | None = None
) -> tuple[pa.Table, int]:
    """Read the configured gold dataset via the Delta transaction log.

    Args:
        config: Identifies the exact table to read.
        version: Exact Delta version to pin; defaults to the table's latest.

    Returns:
        The dataset as a PyArrow table, and the exact Delta version read.
    """
    from deltalake import DeltaTable

    delta_table = DeltaTable(
        config.source_path(), version=version, storage_options=_s3_storage_options()
    )
    table = delta_table.to_pyarrow_table()
    return table, delta_table.version()


def assert_business_keys_unique(
    table: pa.Table, business_keys: tuple[str, ...]
) -> None:
    """Raise if the exported snapshot has duplicate business-key rows.

    Args:
        table: The exact rows about to be published.
        business_keys: Column names that together must be unique.

    Raises:
        ValueError: If any business-key combination repeats.
    """
    if table.num_rows == 0:
        return
    key_table = table.select(list(business_keys))
    distinct_rows = key_table.group_by(list(business_keys)).aggregate([])
    if distinct_rows.num_rows != table.num_rows:
        raise ValueError(
            f"Duplicate business keys {business_keys} in export "
            f"({table.num_rows} rows, {distinct_rows.num_rows} distinct)"
        )


def _write_parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def export_dataset(
    config: GoldExportConfig,
    table: pa.Table,
    batch_id: str,
) -> ManifestDataset:
    """Write one immutable Parquet part file and return its manifest entry.

    Args:
        config: Identifies the dataset and target bucket.
        table: The exact, already-validated rows to publish.
        batch_id: This export's immutable batch identifier.

    Returns:
        The dataset's manifest entry, including its one file's checksum.
    """
    body = _write_parquet_bytes(table)
    key = f"{config.publish_prefix(batch_id)}/part-00000.parquet"
    client = get_s3_client()
    client.put_object(
        Bucket=config.bucket, Key=key, Body=body, ContentType="application/octet-stream"
    )
    logger.info("Published %s (%d rows, %d bytes)", key, table.num_rows, len(body))
    columns = [
        ManifestColumn(name=field.name, type=str(field.type), nullable=field.nullable)
        for field in table.schema
    ]
    file_entry = ManifestFile(
        key=key,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
        row_count=table.num_rows,
    )
    return ManifestDataset(
        name=config.dataset,
        row_count=table.num_rows,
        business_keys=list(DATASET_BUSINESS_KEYS[config.dataset]),
        columns=columns,
        files=[file_entry],
    )


def run_export(
    config: GoldExportConfig,
    producer_run_id: str,
    batch_id: str | None = None,
    version: int | None = None,
    allow_empty: bool = False,
) -> Manifest:
    """Export one dataset as a new immutable, manifest-completed batch.

    Args:
        config: Identifies the exact source table and target bucket.
        producer_run_id: Non-secret identifier for this run (e.g. a CI run ID).
        batch_id: Explicit batch ID; generated when omitted. Never reuse a
            batch_id for different data - retries must pick a new one unless
            they are re-publishing byte-identical content.
        version: Exact Delta version to pin; defaults to latest.
        allow_empty: Must be set explicitly to publish a zero-row snapshot.

    Returns:
        The completed manifest, already written to S3 as the last step.

    Raises:
        ValueError: On duplicate business keys or an unauthorized empty export.
    """
    if config.dataset not in DATASET_BUSINESS_KEYS:
        raise ValueError(f"Unregistered dataset for export: {config.dataset}")
    batch_id = batch_id or _new_batch_id()
    table, source_version = read_gold_table(config, version=version)
    if table.num_rows == 0 and not allow_empty:
        raise ValueError(
            f"Refusing an empty {config.dataset} snapshot; pass allow_empty=True "
            "if this is an intentionally empty batch"
        )
    assert_business_keys_unique(table, DATASET_BUSINESS_KEYS[config.dataset])
    dataset_entry = export_dataset(config, table, batch_id)

    cutoff_column = DATASET_CUTOFF_COLUMN[config.dataset]
    if table.num_rows > 0:
        logical_cutoff = str(pc.max(table.column(cutoff_column)).as_py())
    else:
        logical_cutoff = "empty"

    manifest = Manifest(
        batch_id=batch_id,
        created_at=datetime.now(timezone.utc),
        producer_run_id=producer_run_id,
        logical_cutoff=logical_cutoff,
        publication_sequence=[config.dataset],
        source_tables={
            config.dataset: SourceTableRef(
                table=config.table_identifier(),
                source_path=config.source_path(),
                version=source_version,
            )
        },
        datasets=[dataset_entry],
    )
    verify_published_batch(config.bucket, manifest)
    manifest_key = f"publish/batches/{batch_id}/manifest.json"
    if not upload_json_to_s3(
        manifest.model_dump(mode="json"), config.bucket, manifest_key
    ):
        raise RuntimeError(f"Failed to publish manifest for batch {batch_id}")
    logger.info("Batch %s completed: %s", batch_id, manifest_key)
    return manifest


def verify_published_batch(bucket: str, manifest: Manifest) -> None:
    """Re-derive each listed file's checksum/size/prefix and compare.

    This is the "executable manifest validation" the contract requires
    before a batch may be declared complete - a nonempty S3 prefix alone is
    never sufficient proof.

    Args:
        bucket: Bucket the manifest's files were published to.
        manifest: The manifest to verify, before its own file is written.

    Raises:
        ValueError: If any file is missing, mis-sized, or checksum-mismatched,
            or if a key falls outside this batch's own prefix.
    """
    expected_prefix = f"publish/batches/{manifest.batch_id}/"
    client = get_s3_client()
    for dataset in manifest.datasets:
        total_rows = 0
        for file_entry in dataset.files:
            if not file_entry.key.startswith(expected_prefix):
                raise ValueError(
                    f"Manifest file {file_entry.key} is outside batch prefix {expected_prefix}"
                )
            body = client.get_object(Bucket=bucket, Key=file_entry.key)["Body"].read()
            if len(body) != file_entry.size_bytes:
                raise ValueError(f"Size mismatch for {file_entry.key}")
            if hashlib.sha256(body).hexdigest() != file_entry.sha256:
                raise ValueError(f"Checksum mismatch for {file_entry.key}")
            total_rows += file_entry.row_count
        if total_rows != dataset.row_count:
            raise ValueError(
                f"Row count mismatch for dataset {dataset.name}: "
                f"files sum to {total_rows}, manifest declares {dataset.row_count}"
            )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a gold Delta table as an immutable snapshot"
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument(
        "--run-id", required=True, help="Non-secret identifier for this run"
    )
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--version", type=int, default=None)
    parser.add_argument("--allow-empty", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    config = GoldExportConfig(
        catalog=args.catalog,
        schema_prefix=args.schema_prefix,
        bucket=args.bucket,
        dataset=args.dataset,
    )
    run_export(
        config,
        producer_run_id=args.run_id,
        batch_id=args.batch_id,
        version=args.version,
        allow_empty=args.allow_empty,
    )


if __name__ == "__main__":
    main()
