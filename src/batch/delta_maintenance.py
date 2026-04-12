"""Monthly Delta Lake maintenance for gold-layer tables.

Performs three maintenance operations on every gold Delta table:

1. **OPTIMIZE** — compacts small files produced by daily MERGE
   operations into larger, more efficient Parquet files.  This
   improves read performance for the dashboard and downstream
   analytics.

2. **VACUUM** — removes data files that are no longer referenced
   by the Delta transaction log and are older than the configured
   retention period (default 7 days).  Reclaims storage.

3. **History cleanup** — logs the current version and history
   length for observability.  Delta keeps the transaction log
   indefinitely; VACUUM only removes data files.

Designed to be run monthly via the ``delta_maintenance`` Airflow
DAG.  Safe to re-run — all operations are idempotent.
"""

import argparse
import logging

from pyspark.sql import SparkSession

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# All gold-layer Delta tables managed by this pipeline.
GOLD_TABLES: list[str] = [
    "daily_summaries",
    "sector_performance",
    "correlations",
    "fundamentals",
    "enriched_prices",
]

# Default VACUUM retention in hours (7 days).
DEFAULT_RETENTION_HOURS: int = 168


def create_spark_session(
    app_name: str = "DeltaMaintenance",
) -> SparkSession:
    """Create a SparkSession with Delta Lake extensions.

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    settings = get_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled",
            "true",
        )
    )

    if settings.aws_access_key_id:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                settings.aws_secret_access_key,
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                f"s3.{settings.aws_default_region}.amazonaws.com",
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
        )

    return builder.getOrCreate()


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    """Check whether a Delta table exists at *path*.

    Args:
        spark: Active SparkSession.
        path: S3a path to check.

    Returns:
        ``True`` if a Delta table exists, ``False`` otherwise.
    """
    from delta.tables import DeltaTable

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def optimize_table(spark: SparkSession, path: str) -> None:
    """Compact small files in a Delta table.

    Uses ``OPTIMIZE`` SQL command which rewrites small data files
    into fewer, larger files (target ~1 GB each) without changing
    the logical contents.

    Args:
        spark: Active SparkSession.
        path: S3a path to the Delta table.
    """
    spark.sql(f"OPTIMIZE delta.`{path}`")
    logger.info("OPTIMIZE complete for %s", path)


def vacuum_table(
    spark: SparkSession, path: str, retention_hours: int = DEFAULT_RETENTION_HOURS
) -> None:
    """Remove stale data files no longer referenced by the Delta log.

    Args:
        spark: Active SparkSession.
        path: S3a path to the Delta table.
        retention_hours: Files older than this are eligible for deletion.
            Must be >= 168 (7 days) unless the safety check is disabled.
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info("VACUUM complete for %s (retention=%dh)", path, retention_hours)


def log_table_history(spark: SparkSession, path: str) -> dict[str, int]:
    """Log the Delta table version and history length.

    Args:
        spark: Active SparkSession.
        path: S3a path to the Delta table.

    Returns:
        Dict with ``current_version`` and ``history_entries``.
    """
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(spark, path)
    history_df = dt.history()
    history_count = history_df.count()
    current_version = history_df.selectExpr("max(version)").collect()[0][0]

    logger.info(
        "Delta history for %s: version=%s, entries=%d",
        path,
        current_version,
        history_count,
    )
    return {"current_version": current_version, "history_entries": history_count}


def run_maintenance(
    spark: SparkSession | None = None,
    retention_hours: int = DEFAULT_RETENTION_HOURS,
) -> dict[str, dict[str, int]]:
    """Run full maintenance cycle on all gold Delta tables.

    For each table: OPTIMIZE → VACUUM → log history.
    Tables that don't exist yet (e.g. first deployment) are skipped.

    Args:
        spark: SparkSession. Created if not provided.
        retention_hours: VACUUM retention period in hours.

    Returns:
        Dict mapping table name → history info.
    """
    settings = get_settings()

    if spark is None:
        spark = create_spark_session()

    bucket = settings.s3_bucket_name
    gold_path = f"s3a://{bucket}/gold"

    results: dict[str, dict[str, int]] = {}

    for table_name in GOLD_TABLES:
        path = f"{gold_path}/{table_name}"

        if not _is_delta_table(spark, path):
            logger.warning("Skipping %s — not a Delta table (yet)", path)
            continue

        logger.info("Starting maintenance for gold/%s", table_name)

        optimize_table(spark, path)
        vacuum_table(spark, path, retention_hours)
        info = log_table_history(spark, path)
        results[table_name] = info

        logger.info("Maintenance complete for gold/%s", table_name)

    logger.info(
        "Delta maintenance finished: %d/%d tables processed",
        len(results),
        len(GOLD_TABLES),
    )
    return results


def main() -> None:
    """Entry point for the Delta maintenance job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Delta Lake monthly maintenance")
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=DEFAULT_RETENTION_HOURS,
        help="VACUUM retention period in hours (default: 168 = 7 days).",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run_maintenance(spark, retention_hours=args.retention_hours)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
