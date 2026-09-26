"""Snowflake snapshot loader: loads the latest completed gold snapshot batch
(published by src/export/gold_snapshot.py) into Snowflake, per the "Snapshot
Load Contract" in .github/instructions/snowflake.instructions.md.

Runs as the least-privilege loader role created by
snowflake/terraform/workspace/main.tf (never ACCOUNTADMIN), authenticating
with key-pair (JWT) auth - the same mechanism the Terraform provider uses.

Flow, matching the contract:
  1. Find the latest *completed* batch (a manifest.json exists and declares
     ``status: completed``) - an incomplete batch (data files but no
     manifest, or a non-completed manifest) is never a load candidate.
  2. Re-validate the manifest's declared schema/business-keys against this
     module's own expectations (catches upstream schema drift before it
     reaches SQL), then re-derive every file's checksum via
     ``gold_snapshot.verify_published_batch`` - the loader trusts nothing
     the exporter merely claims.
  3. Consult the BATCH_LEDGER control table: an already-completed batch_id
     is a no-op (safe replay); a batch whose source version is not newer
     than the latest already-completed version is rejected as stale.
  4. ``COPY INTO`` an isolated staging table from the stage's exact
     manifest-listed files (never the whole prefix), casting each column
     explicitly - never trusting Parquet-inferred types.
  5. Validate staged row count and business-key uniqueness in Snowflake
     itself (a standard table's PK/unique declarations are not enforced).
  6. Publish: one transaction does ``DELETE`` + ``INSERT`` into the serving
     table and marks the ledger row completed together - no ``TRUNCATE`` or
     ``CREATE OR REPLACE`` inside it (both implicitly commit in Snowflake).
     A failure anywhere rolls back, leaving the previous serving snapshot
     intact, and records a ``failed`` ledger row for visibility.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from src.common.s3_utils import get_s3_client
from src.export.gold_snapshot import (
    DATASET_BUSINESS_KEYS,
    Manifest,
    ManifestDataset,
    verify_published_batch,
)

logger = logging.getLogger(__name__)

IDENTIFIER = r"^[A-Za-z_][A-Za-z0-9_]*$"

SQL_DIR = Path(__file__).resolve().parents[2] / "snowflake" / "sql"
CONTROL_SQL_FILE = "001_control_batch_ledger.sql"

# Per-dataset registry: staging/serving table names, the DDL files that
# create them, and the exact expected columns with their Snowflake cast
# type for COPY INTO. Extend all four together when adding a dataset -
# never infer a table's shape implicitly from an incoming manifest.
DATASET_STAGING_TABLE: dict[str, str] = {
    "daily_quote_summary": "DAILY_QUOTE_SUMMARY_STAGING",
}
DATASET_SERVING_TABLE: dict[str, str] = {
    "daily_quote_summary": "DAILY_QUOTE_SUMMARY",
}
DATASET_SQL_FILES: dict[str, tuple[str, str]] = {
    "daily_quote_summary": (
        "002_staging_daily_quote_summary.sql",
        "003_serving_daily_quote_summary.sql",
    ),
}
DATASET_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "daily_quote_summary": (
        ("provider", "VARCHAR"),
        ("symbol", "VARCHAR"),
        ("capture_date_utc", "DATE"),
        ("first_observed_price", "DOUBLE"),
        ("highest_observed_price", "DOUBLE"),
        ("lowest_observed_price", "DOUBLE"),
        ("last_observed_price", "DOUBLE"),
        ("last_reported_volume", "NUMBER(38,0)"),
        ("quote_count", "NUMBER(38,0)"),
        ("first_quote_at", "TIMESTAMP_NTZ"),
        ("last_quote_at", "TIMESTAMP_NTZ"),
        ("observed_change_pct", "DOUBLE"),
    ),
}


class SnowflakeLoadConfig(BaseModel):
    """Identifies the exact Snowflake session and dataset this loader targets.

    Attributes:
        account: Snowflake account identifier (e.g. ``ORG-ACCOUNT``).
        user: Snowflake username to authenticate as (key-pair auth).
        role: Least-privilege loader role to assume for the session.
        warehouse: Warehouse providing compute for this load.
        database: Existing database created by the workspace Terraform root.
        staging_schema: Schema holding staging + the BATCH_LEDGER control table.
        serving_schema: Schema holding the published, queryable output table.
        stage_name: Fully-qualified external stage (``DB.SCHEMA.STAGE``).
        bucket: S3 bucket the stage/manifests read from.
        dataset: Registered dataset name; must exist in DATASET_COLUMNS.
    """

    account: str = Field(min_length=1)
    user: str = Field(min_length=1)
    role: str = Field(pattern=IDENTIFIER)
    warehouse: str = Field(pattern=IDENTIFIER)
    database: str = Field(pattern=IDENTIFIER)
    staging_schema: str = Field(pattern=IDENTIFIER)
    serving_schema: str = Field(pattern=IDENTIFIER)
    stage_name: str = Field(min_length=1)
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    dataset: str

    def qualified(self, schema: str, table: str) -> str:
        """Return a fully-qualified ``database.schema.table`` identifier."""
        return f"{self.database}.{schema}.{table}"


class LoadResult(BaseModel):
    dataset: str
    batch_id: str
    status: str  # "completed" | "skipped_already_loaded"
    rows_loaded: int


def _private_key_der(pem_text: str, passphrase: str | None) -> bytes:
    """Convert a PEM private key to the DER/PKCS8 bytes the connector wants."""
    from cryptography.hazmat.primitives import serialization

    password = passphrase.encode("utf-8") if passphrase else None
    key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"), password=password
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(
    config: SnowflakeLoadConfig,
    private_key_pem: str,
    private_key_passphrase: str | None = None,
) -> Any:
    """Open a key-pair authenticated Snowflake connection as the loader role."""
    import snowflake.connector as sf

    return sf.connect(
        account=config.account,
        user=config.user,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.staging_schema,
        private_key=_private_key_der(private_key_pem, private_key_passphrase),
    )


def find_latest_batch_id(bucket: str, prefix: str = "publish/batches/") -> str | None:
    """Return the most recent batch_id with a completed manifest, if any.

    Batch IDs sort chronologically (see gold_snapshot._new_batch_id), so
    candidates are checked newest-first; a batch with data files but no
    manifest yet (in-flight publish) is skipped, not treated as latest.
    """
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    batch_ids: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            batch_id = common["Prefix"][len(prefix) :].rstrip("/")
            if batch_id:
                batch_ids.append(batch_id)

    for batch_id in sorted(batch_ids, reverse=True):
        try:
            client.head_object(Bucket=bucket, Key=f"{prefix}{batch_id}/manifest.json")
            return batch_id
        except ClientError:
            continue
    return None


def load_manifest(bucket: str, batch_id: str) -> Manifest:
    """Fetch and parse one batch's manifest.json; reject a non-completed one."""
    client = get_s3_client()
    body = client.get_object(
        Bucket=bucket, Key=f"publish/batches/{batch_id}/manifest.json"
    )["Body"].read()
    manifest = Manifest.model_validate(json.loads(body))
    if manifest.status != "completed":
        raise ValueError(
            f"Batch {batch_id} manifest status is {manifest.status!r}, not completed"
        )
    return manifest


def _dataset_entry(manifest: Manifest, dataset: str) -> ManifestDataset:
    for entry in manifest.datasets:
        if entry.name == dataset:
            return entry
    raise ValueError(f"Batch {manifest.batch_id} manifest has no dataset {dataset!r}")


def validate_expected_schema(entry: ManifestDataset, dataset: str) -> None:
    """Reject a manifest whose columns/business-keys drifted from this loader's
    fixed expectations - schema changes need an explicit loader update, not a
    silent implicit load of a differently-shaped table."""
    expected_columns = {name for name, _ in DATASET_COLUMNS[dataset]}
    actual_columns = {c.name for c in entry.columns}
    if actual_columns != expected_columns:
        raise ValueError(
            f"Schema drift for {dataset}: expected columns "
            f"{sorted(expected_columns)}, manifest has {sorted(actual_columns)}"
        )
    expected_keys = set(DATASET_BUSINESS_KEYS[dataset])
    if set(entry.business_keys) != expected_keys:
        raise ValueError(
            f"Business-key drift for {dataset}: expected {sorted(expected_keys)}, "
            f"manifest declares {sorted(entry.business_keys)}"
        )


def _execute_sql_file(cursor: Any, filename: str, **format_args: str) -> None:
    statement = (SQL_DIR / filename).read_text().format(**format_args)
    cursor.execute(statement)


def ensure_objects_exist(conn: Any, config: SnowflakeLoadConfig) -> None:
    """Idempotently create the control/staging/serving tables this dataset
    needs, from the versioned SQL in snowflake/sql/ - never CREATE OR REPLACE,
    so a re-run can never silently empty an existing table."""
    cursor = conn.cursor()
    try:
        _execute_sql_file(
            cursor,
            CONTROL_SQL_FILE,
            database=config.database,
            staging_schema=config.staging_schema,
        )
        for filename in DATASET_SQL_FILES[config.dataset]:
            _execute_sql_file(
                cursor,
                filename,
                database=config.database,
                staging_schema=config.staging_schema,
                serving_schema=config.serving_schema,
            )
    finally:
        cursor.close()


def get_ledger_row(
    conn: Any, config: SnowflakeLoadConfig, batch_id: str
) -> dict[str, Any] | None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT status, source_version FROM "
            f"{config.qualified(config.staging_schema, 'BATCH_LEDGER')} "
            "WHERE dataset = %s AND batch_id = %s",
            (config.dataset, batch_id),
        )
        row = cursor.fetchone()
        return None if row is None else {"status": row[0], "source_version": row[1]}
    finally:
        cursor.close()


def get_latest_completed_version(conn: Any, config: SnowflakeLoadConfig) -> int | None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MAX(source_version) FROM "
            f"{config.qualified(config.staging_schema, 'BATCH_LEDGER')} "
            "WHERE dataset = %s AND status = 'completed'",
            (config.dataset,),
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else None
    finally:
        cursor.close()


def start_batch(
    conn: Any, config: SnowflakeLoadConfig, manifest: Manifest, entry: ManifestDataset
) -> None:
    """Upsert a 'loading' ledger row - MERGE so a retry of a failed batch_id
    reuses the same row instead of violating the (dataset, batch_id) key."""
    source_ref = manifest.source_tables[config.dataset]
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            MERGE INTO {config.qualified(config.staging_schema, 'BATCH_LEDGER')} t
            USING (SELECT %s AS dataset, %s AS batch_id) s
            ON t.dataset = s.dataset AND t.batch_id = s.batch_id
            WHEN MATCHED THEN UPDATE SET
                status = 'loading', source_table = %s, source_version = %s,
                manifest_row_count = %s, started_at = CURRENT_TIMESTAMP(),
                completed_at = NULL, error_message = NULL
            WHEN NOT MATCHED THEN INSERT
                (dataset, batch_id, source_table, source_version, manifest_row_count, status, started_at)
                VALUES (%s, %s, %s, %s, %s, 'loading', CURRENT_TIMESTAMP())
            """,
            (
                config.dataset,
                manifest.batch_id,
                source_ref.table,
                source_ref.version,
                entry.row_count,
                config.dataset,
                manifest.batch_id,
                source_ref.table,
                source_ref.version,
                entry.row_count,
            ),
        )
    finally:
        cursor.close()


def complete_batch(
    conn: Any, config: SnowflakeLoadConfig, batch_id: str, loaded_row_count: int
) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {config.qualified(config.staging_schema, 'BATCH_LEDGER')} "
            "SET status = 'completed', loaded_row_count = %s, completed_at = CURRENT_TIMESTAMP() "
            "WHERE dataset = %s AND batch_id = %s",
            (loaded_row_count, config.dataset, batch_id),
        )
    finally:
        cursor.close()


def fail_batch(
    conn: Any, config: SnowflakeLoadConfig, batch_id: str, message: str
) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {config.qualified(config.staging_schema, 'BATCH_LEDGER')} "
            "SET status = 'failed', error_message = %s, completed_at = CURRENT_TIMESTAMP() "
            "WHERE dataset = %s AND batch_id = %s",
            (message[:4000], config.dataset, batch_id),
        )
    finally:
        cursor.close()


def copy_into_staging(
    conn: Any, config: SnowflakeLoadConfig, entry: ManifestDataset, batch_id: str
) -> None:
    """Load the exact manifest-listed files into staging, casting every
    column explicitly from the Parquet VARIANT rather than trusting an
    inferred type - never point COPY INTO at the whole batch/dataset prefix."""
    columns = DATASET_COLUMNS[config.dataset]
    staging_fqn = config.qualified(
        config.staging_schema, DATASET_STAGING_TABLE[config.dataset]
    )
    select_list = ", ".join(f"$1:{name}::{cast}" for name, cast in columns)
    column_list = ", ".join(name for name, _ in columns)

    expected_prefix = f"publish/batches/{batch_id}/"
    relative_files = []
    for file_entry in entry.files:
        if not file_entry.key.startswith(expected_prefix):
            raise ValueError(
                f"Manifest file {file_entry.key} is outside batch prefix {expected_prefix}"
            )
        relative_files.append(file_entry.key[len("publish/") :])
    files_clause = ", ".join(f"'{key}'" for key in relative_files)

    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {staging_fqn}")
        cursor.execute(
            f"COPY INTO {staging_fqn} ({column_list}) "
            f"FROM (SELECT {select_list} FROM @{config.stage_name}) "
            f"FILES = ({files_clause}) "
            "FILE_FORMAT = (TYPE = PARQUET) "
            "ON_ERROR = ABORT_STATEMENT"
        )
    finally:
        cursor.close()


def validate_staging(
    conn: Any, config: SnowflakeLoadConfig, entry: ManifestDataset
) -> int:
    """Confirm the staged row count matches the manifest and business keys
    are unique - Snowflake standard tables do not enforce either on their own."""
    staging_fqn = config.qualified(
        config.staging_schema, DATASET_STAGING_TABLE[config.dataset]
    )
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {staging_fqn}")
        actual_count = cursor.fetchone()[0]
        if actual_count != entry.row_count:
            raise ValueError(
                f"Staging row count mismatch for {config.dataset}: "
                f"loaded {actual_count}, manifest declares {entry.row_count}"
            )
        keys = DATASET_BUSINESS_KEYS[config.dataset]
        key_list = ", ".join(keys)
        cursor.execute(
            f"SELECT {key_list}, COUNT(*) FROM {staging_fqn} "
            f"GROUP BY {key_list} HAVING COUNT(*) > 1 LIMIT 1"
        )
        if cursor.fetchone() is not None:
            raise ValueError(
                f"Duplicate business keys {keys} found in staged {config.dataset}"
            )
        return actual_count
    finally:
        cursor.close()


def publish_serving(conn: Any, config: SnowflakeLoadConfig, batch_id: str) -> None:
    """Atomically replace the serving table's contents from staging.

    DML-only (DELETE + INSERT), never TRUNCATE/CREATE OR REPLACE - both
    implicitly commit in Snowflake, which would break this transaction's
    all-or-nothing guarantee. A failure rolls back, leaving the previous
    published snapshot intact.
    """
    columns = [name for name, _ in DATASET_COLUMNS[config.dataset]]
    staging_fqn = config.qualified(
        config.staging_schema, DATASET_STAGING_TABLE[config.dataset]
    )
    serving_fqn = config.qualified(
        config.serving_schema, DATASET_SERVING_TABLE[config.dataset]
    )
    column_list = ", ".join(columns)

    cursor = conn.cursor()
    try:
        conn.autocommit(False)
        cursor.execute(f"DELETE FROM {serving_fqn}")
        cursor.execute(
            f"INSERT INTO {serving_fqn} ({column_list}, loaded_batch_id, loaded_at) "
            f"SELECT {column_list}, %s, CURRENT_TIMESTAMP() FROM {staging_fqn}",
            (batch_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit(True)
        cursor.close()


def run_load(
    config: SnowflakeLoadConfig,
    private_key_pem: str,
    batch_id: str | None = None,
    private_key_passphrase: str | None = None,
) -> LoadResult:
    """Load one batch (the latest completed one, unless batch_id is given).

    Args:
        config: Identifies the Snowflake session, stage, bucket, and dataset.
        private_key_pem: PEM-encoded private key matching the loader user's
            registered public key (never logged or persisted by this module).
        batch_id: Explicit batch to load; defaults to the latest completed one.
        private_key_passphrase: Passphrase for an encrypted private key, if any.

    Returns:
        The outcome: "completed" (rows actually published) or
        "skipped_already_loaded" (idempotent replay of a prior success).

    Raises:
        ValueError: On an unregistered dataset, no completed batch found,
            schema/business-key drift, a stale/duplicate source version, or
            a validation failure after COPY INTO (row count/uniqueness) -
            each of these leaves the ledger row (if started) marked 'failed'
            and the previous serving snapshot untouched.
    """
    if config.dataset not in DATASET_COLUMNS:
        raise ValueError(f"Unregistered dataset for loading: {config.dataset}")

    resolved_batch_id = batch_id or find_latest_batch_id(config.bucket)
    if resolved_batch_id is None:
        raise ValueError(
            f"No completed batch found under publish/batches/ in {config.bucket}"
        )

    manifest = load_manifest(config.bucket, resolved_batch_id)
    entry = _dataset_entry(manifest, config.dataset)
    validate_expected_schema(entry, config.dataset)
    verify_published_batch(config.bucket, manifest)

    conn = connect(config, private_key_pem, private_key_passphrase)
    try:
        ensure_objects_exist(conn, config)

        existing = get_ledger_row(conn, config, resolved_batch_id)
        if existing is not None and existing["status"] == "completed":
            logger.info(
                "Batch %s already loaded for %s; no-op",
                resolved_batch_id,
                config.dataset,
            )
            return LoadResult(
                dataset=config.dataset,
                batch_id=resolved_batch_id,
                status="skipped_already_loaded",
                rows_loaded=0,
            )

        source_version = manifest.source_tables[config.dataset].version
        latest_completed_version = get_latest_completed_version(conn, config)
        if (
            latest_completed_version is not None
            and source_version <= latest_completed_version
        ):
            raise ValueError(
                f"Stale or duplicate batch {resolved_batch_id}: source version "
                f"{source_version} <= already-loaded version {latest_completed_version}"
            )

        start_batch(conn, config, manifest, entry)
        try:
            copy_into_staging(conn, config, entry, resolved_batch_id)
            loaded_rows = validate_staging(conn, config, entry)
            publish_serving(conn, config, resolved_batch_id)
            complete_batch(conn, config, resolved_batch_id, loaded_rows)
        except Exception as exc:
            fail_batch(conn, config, resolved_batch_id, str(exc))
            raise

        logger.info(
            "Batch %s completed: %d rows published to %s.%s.%s",
            resolved_batch_id,
            loaded_rows,
            config.database,
            config.serving_schema,
            DATASET_SERVING_TABLE[config.dataset],
        )
        return LoadResult(
            dataset=config.dataset,
            batch_id=resolved_batch_id,
            status="completed",
            rows_loaded=loaded_rows,
        )
    finally:
        conn.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the latest completed gold snapshot batch into Snowflake"
    )
    parser.add_argument("--account", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--serving-schema", required=True)
    parser.add_argument(
        "--stage-name",
        required=True,
        help="Fully-qualified external stage, e.g. DATABASE.SCHEMA.STAGE_NAME",
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Explicit batch to load; defaults to the latest completed batch",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    import os

    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    config = SnowflakeLoadConfig(
        account=args.account,
        user=args.user,
        role=args.role,
        warehouse=args.warehouse,
        database=args.database,
        staging_schema=args.staging_schema,
        serving_schema=args.serving_schema,
        stage_name=args.stage_name,
        bucket=args.bucket,
        dataset=args.dataset,
    )
    private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    result = run_load(
        config,
        private_key_pem,
        batch_id=args.batch_id,
        private_key_passphrase=passphrase,
    )
    logger.info("Load result: %s", result.model_dump())


if __name__ == "__main__":
    main()
