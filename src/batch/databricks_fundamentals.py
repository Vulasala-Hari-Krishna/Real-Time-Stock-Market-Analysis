"""Manual Databricks fundamentals vertical slice using runtime Spark and Delta.

R6's first migrated legacy product (fundamental_enrichment.py). Unlike the
ticks slice, there is no S3 landing/Auto Loader stage here: the job fetches
yfinance directly (see landed_fundamentals.py::fetch_all) and appends the
results straight to bronze. This mirrors how the legacy Spark job already
fetched yfinance from inside itself, and keeps this business logic running
on Databricks compute - not on a GitHub Actions runner (which owns infra
deploy/update/teardown here, never job execution; see landed_fundamentals.py's
module docstring for why a GitHub-Actions-hosted fetch was tried and
abandoned).

Every run is a genuine new observation (there is no landed-file checkpoint
to make a run idempotent against), so this job always fetches and rebuilds
gold - unlike databricks_ticks.py, there is no "snapshot already current"
skip.
"""

import argparse
import logging

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.landed_fundamentals import (
    BRONZE_SCHEMA,
    fetch_all,
    project_fundamentals,
    rank_latest_per_symbol,
)
from src.config.watchlist import SYMBOLS

logger = logging.getLogger(__name__)
PIPELINE_REVISION = "fundamentals-v2-direct-fetch"

TABLES = {
    "raw": ("bronze", "fundamentals_raw"),
    "summary": ("gold", "fundamentals"),
    "state": ("gold", "fundamentals_pipeline_state"),
}
STATE_SCHEMA = (
    "status string, pipeline_revision string, bronze_version long, "
    "fetched_rows long, skipped_rows long, gold_rows long, summary_version long"
)


class FundamentalsJobConfig(BaseModel):
    """Explicit job parameters with isolated storage and safe UC identifiers.

    Attributes:
        catalog: Existing Unity Catalog catalog.
        schema_prefix: Prefix for existing bronze/gold schemas.
        bucket: Existing S3 bucket, accessed through Unity Catalog.
        max_input_rows: Full-rebuild safety limit on accumulated bronze history.
    """

    catalog: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_prefix: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    max_input_rows: int = Field(default=100000, ge=1, le=1000000)

    def table(self, dataset: str) -> str:
        """Return the qualified identifier for an owned dataset."""
        layer, name = TABLES[dataset]
        return f"{self.catalog}.{self.schema_prefix}_{layer}.{name}"

    def path(self, dataset: str) -> str:
        """Return its isolated external Delta storage location."""
        layer, name = TABLES[dataset]
        return (
            f"s3://{self.bucket}/lakehouse/{layer}/"
            f"{self.catalog}/{self.schema_prefix}/{name}"
        )


def validate_locations(spark: SparkSession, config: FundamentalsJobConfig) -> None:
    """Reject existing tables with the wrong format or external location.

    Args:
        spark: Runtime-provided Spark session.
        config: Expected storage and table identities.

    Raises:
        ValueError: If a target name belongs to an unexpected table.
    """
    for dataset in TABLES:
        table = config.table(dataset)
        if spark.catalog.tableExists(table):
            detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
            if (
                detail is None
                or detail["format"] != "delta"
                or detail["location"].rstrip("/") != config.path(dataset)
            ):
                raise ValueError(f"Refusing unexpected table format/location: {table}")


def table_version(spark: SparkSession, table: str) -> int:
    """Read the current Delta version; missing history is a hard failure."""
    history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    if history is None:
        raise ValueError(f"No Delta history for {table}")
    return int(history["version"])


def write_snapshot(
    frame: DataFrame, config: FundamentalsJobConfig, dataset: str
) -> None:
    """Atomically replace one owned Delta dataset, including valid empty results."""
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("path", config.path(dataset))
        .saveAsTable(config.table(dataset))
    )


def append_bronze(
    spark: SparkSession, config: FundamentalsJobConfig, rows: list[dict]
) -> int:
    """Append this run's fetched rows to bronze and return the new version.

    Args:
        spark: Runtime session.
        config: Table identities.
        rows: Fetched/validated rows from fetch_all (never empty - callers
            must refuse to append an empty batch before calling this).

    Returns:
        The bronze table's Delta version after this append.
    """
    frame = spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn(
        "bronze_ingested_at", F.current_timestamp()
    )
    (
        frame.write.format("delta")
        .mode("append")
        .option("path", config.path("raw"))
        .saveAsTable(config.table("raw"))
    )
    return table_version(spark, config.table("raw"))


def rebuild_outputs(
    spark: SparkSession, config: FundamentalsJobConfig, symbols: list[str]
) -> dict[str, int]:
    """Fetch fresh data, append to bronze, and republish gold from all history.

    Args:
        spark: Runtime session; UTC must be configured by the caller.
        config: Table identities and full-rebuild size guard.
        symbols: Ticker symbols to fetch this run.

    Returns:
        Counts of fetched/skipped symbols this run and the resulting gold
        row count.

    Raises:
        RuntimeError: If every symbol failed to fetch - never appends an
            empty batch silently as if it were a legitimate observation.
        ValueError: When the accumulated bronze history exceeds the
            full-rebuild size guard.
    """
    state = ["processing", PIPELINE_REVISION, -1, 0, 0, 0, -1]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    rows = fetch_all(symbols)
    if not rows:
        raise RuntimeError(
            f"All {len(symbols)} symbols failed to fetch; refusing to append an empty bronze batch"
        )
    bronze_version = append_bronze(spark, config, rows)
    bronze = spark.read.option("versionAsOf", bronze_version).table(config.table("raw"))
    total_rows = bronze.limit(config.max_input_rows + 1).count()
    if total_rows > config.max_input_rows:
        raise ValueError(
            "Accumulated bronze history exceeds the full-rebuild row limit; "
            "incremental design required"
        )
    latest = rank_latest_per_symbol(bronze)
    gold = project_fundamentals(latest).withColumn(
        "bronze_version", F.lit(bronze_version)
    )
    write_snapshot(gold, config, "summary")
    gold_rows = gold.count()
    counts = {
        "fetched": len(rows),
        "skipped": len(symbols) - len(rows),
        "gold_rows": gold_rows,
    }
    state = [
        "completed",
        PIPELINE_REVISION,
        bronze_version,
        counts["fetched"],
        counts["skipped"],
        gold_rows,
        table_version(spark, config.table("summary")),
    ]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    logger.info("Completed bronze_version=%s counts=%s", bronze_version, counts)
    return counts


def run(
    spark: SparkSession, config: FundamentalsJobConfig, symbols: list[str] | None = None
) -> dict[str, int]:
    """Fetch and rebuild - every run is a new observation, never a no-op."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    validate_locations(spark, config)
    return rebuild_outputs(spark, config, symbols or SYMBOLS)


def main() -> None:
    """Run only when explicitly invoked by a Databricks wheel task."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--max-input-rows", type=int, default=100000)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated override; defaults to the watchlist",
    )
    args = parser.parse_args()
    config = FundamentalsJobConfig(
        catalog=args.catalog,
        schema_prefix=args.schema_prefix,
        bucket=args.bucket,
        max_input_rows=args.max_input_rows,
    )
    symbols = args.symbols.split(",") if args.symbols else None
    logging.basicConfig(level=logging.INFO)
    run(SparkSession.builder.getOrCreate(), config, symbols=symbols)


if __name__ == "__main__":
    main()
