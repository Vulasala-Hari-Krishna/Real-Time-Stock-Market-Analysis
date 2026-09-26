"""Manual Databricks indicators/signals/sector/correlation vertical slice
using runtime Spark and Delta.

R6's third migrated legacy product (daily_aggregation.py). Reuses that
module's indicator/signal/sector/correlation transform functions directly
(compute_daily_summaries, compute_sector_performance,
compute_correlation_matrix) - they are pure DataFrame transforms with no
S3/path-specific logic and already have real-Spark test coverage from the
legacy job, so porting them means importing them, not re-implementing them.

Unlike every other job in this migration, this one has no bronze/fetch
stage of its own: its input is databricks_historical.py's already-published
gold ``historical_ohlcv`` table. Every run recomputes fully from that
table's current version - the same "always rebuild, no incremental
MERGE/skip-if-unchanged" simplification used throughout this migration,
deliberately dropping the legacy job's "daily incremental MERGE" mode for
consistency and simplicity at this project's data scale.
"""

import argparse
import logging

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.daily_aggregation import (
    compute_correlation_matrix,
    compute_daily_summaries,
    compute_sector_performance,
)
from src.batch.databricks_common import DELTA_AUTO_OPTIMIZE_PROPERTIES

logger = logging.getLogger(__name__)
PIPELINE_REVISION = "indicators-v1-full-rebuild"

TABLES = {
    "daily_summaries": ("gold", "daily_summaries"),
    "sector_performance": ("gold", "sector_performance"),
    "correlations": ("gold", "correlations"),
    "state": ("gold", "indicators_pipeline_state"),
}
STATE_SCHEMA = (
    "status string, pipeline_revision string, source_version long, "
    "daily_summaries_rows long, sector_performance_rows long, correlations_rows long, "
    "daily_summaries_version long, sector_performance_version long, correlations_version long"
)


class IndicatorsJobConfig(BaseModel):
    """Explicit job parameters with isolated storage and safe UC identifiers.

    Attributes:
        catalog: Existing Unity Catalog catalog.
        schema_prefix: Prefix for existing gold schema; also identifies
            databricks_historical.py's ``historical_ohlcv`` source table.
        bucket: Existing S3 bucket, accessed through Unity Catalog.
        max_input_rows: Full-rebuild safety limit on the source table.
    """

    catalog: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_prefix: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    max_input_rows: int = Field(default=200000, ge=1, le=2000000)

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

    def source_table(self) -> str:
        """Return databricks_historical.py's owned gold table identifier."""
        return f"{self.catalog}.{self.schema_prefix}_gold.historical_ohlcv"


def validate_locations(spark: SparkSession, config: IndicatorsJobConfig) -> None:
    """Reject existing tables with the wrong format or external location.

    Only checks this job's own owned tables - the source table
    (historical_ohlcv) is owned and validated by databricks_historical.py.

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


def write_snapshot(frame: DataFrame, config: IndicatorsJobConfig, dataset: str) -> None:
    """Atomically replace one owned Delta dataset, including valid empty results."""
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("path", config.path(dataset))
        .options(**DELTA_AUTO_OPTIMIZE_PROPERTIES)
        .saveAsTable(config.table(dataset))
    )


def read_source(
    spark: SparkSession, config: IndicatorsJobConfig
) -> tuple[DataFrame, int]:
    """Read databricks_historical.py's gold table at its current pinned version.

    Args:
        spark: Runtime session.
        config: Identifies the source table.

    Returns:
        The source rows (only the OHLCV columns these transforms need,
        dropping historical_ohlcv's own lineage columns), and the exact
        version read.
    """
    source_version = table_version(spark, config.source_table())
    df = (
        spark.read.option("versionAsOf", source_version)
        .table(config.source_table())
        .select("symbol", "date", "open", "high", "low", "close", "volume")
    )
    return df, source_version


def rebuild_outputs(spark: SparkSession, config: IndicatorsJobConfig) -> dict[str, int]:
    """Recompute indicators/signals/sector/correlations from the source table.

    Args:
        spark: Runtime session; UTC must be configured by the caller.
        config: Table identities and full-rebuild size guard.

    Returns:
        Row counts for each of the three published gold tables.

    Raises:
        RuntimeError: If the source table has no rows yet - never publishes
            empty gold outputs as if they were a legitimately-computed
            result of a nonexistent source.
        ValueError: When the source table exceeds the full-rebuild size guard.
    """
    state = ["processing", PIPELINE_REVISION, -1, 0, 0, 0, -1, -1, -1]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    source_df, source_version = read_source(spark, config)
    input_rows = source_df.limit(config.max_input_rows + 1).count()
    if input_rows > config.max_input_rows:
        raise ValueError(
            "Source historical_ohlcv exceeds the full-rebuild row limit; "
            "incremental design required"
        )
    if input_rows == 0:
        raise RuntimeError(
            f"Source table {config.source_table()} is empty; "
            "run the historical OHLCV job (databricks_historical.py) first"
        )
    summaries = compute_daily_summaries(source_df)
    sector_perf = compute_sector_performance(summaries)
    correlations = compute_correlation_matrix(summaries, spark)

    write_snapshot(
        summaries.withColumn("source_version", F.lit(source_version)),
        config,
        "daily_summaries",
    )
    write_snapshot(
        sector_perf.withColumn("source_version", F.lit(source_version)),
        config,
        "sector_performance",
    )
    write_snapshot(
        correlations.withColumn("source_version", F.lit(source_version)),
        config,
        "correlations",
    )

    counts = {
        "daily_summaries_rows": summaries.count(),
        "sector_performance_rows": sector_perf.count(),
        "correlations_rows": correlations.count(),
    }
    state = [
        "completed",
        PIPELINE_REVISION,
        source_version,
        counts["daily_summaries_rows"],
        counts["sector_performance_rows"],
        counts["correlations_rows"],
        table_version(spark, config.table("daily_summaries")),
        table_version(spark, config.table("sector_performance")),
        table_version(spark, config.table("correlations")),
    ]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    logger.info("Completed source_version=%s counts=%s", source_version, counts)
    return counts


def run(spark: SparkSession, config: IndicatorsJobConfig) -> dict[str, int]:
    """Recompute from the current source version - every run is a full rebuild."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    validate_locations(spark, config)
    return rebuild_outputs(spark, config)


def main() -> None:
    """Run only when explicitly invoked by a Databricks wheel task."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--max-input-rows", type=int, default=200000)
    args = parser.parse_args()
    config = IndicatorsJobConfig(
        catalog=args.catalog,
        schema_prefix=args.schema_prefix,
        bucket=args.bucket,
        max_input_rows=args.max_input_rows,
    )
    # force=True: Databricks Runtime configures the root logger before this
    # code ever runs, and basicConfig() silently no-ops if the root logger
    # already has handlers (found live 2026-09-26 in databricks_fundamentals.py).
    logging.basicConfig(level=logging.INFO, force=True)
    run(SparkSession.builder.getOrCreate(), config)


if __name__ == "__main__":
    main()
