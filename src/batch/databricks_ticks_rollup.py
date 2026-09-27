"""Databricks ticks-to-historical rollup job runner.

Mirrors the legacy ``tick_rollup.py`` DAG exactly: a cheap, daily,
weekdays-only job that syncs already-captured data into the historical
table - no external API call, ever. Appends ``landed_ticks``' already-
published gold ``daily_quote_summary`` into ``historical_ohlcv``/
``historical_ohlcv_raw`` - the same tables ``databricks_historical.py``
owns for its one-time/manual yfinance backfill (``initial_historical_backfill.py``'s
hybrid equivalent). Two different feeders of one shared target, exactly
mirroring legacy's own split between ``initial_historical_backfill.py`` and
``tick_rollup.py``.

Reuses ``databricks_historical.py``'s config/table/write helpers and
``landed_historical.py``'s ranking/projection transforms unchanged, since
this writes to the identical tables with the identical business key
(symbol, date) - the only new thing here is *where the rows come from*
(``daily_quote_summary``, not yfinance).
"""

import argparse
import logging
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.databricks_common import DELTA_AUTO_OPTIMIZE_PROPERTIES
from src.batch.databricks_historical import (
    STATE_SCHEMA,
    HistoricalJobConfig,
    table_version,
    validate_locations,
    write_snapshot,
)
from src.batch.databricks_ticks import TABLES as TICKS_TABLES
from src.batch.landed_historical import project_historical, rank_latest_per_symbol_date
from src.batch.landed_ticks_rollup import project_quote_summary_as_ohlcv

logger = logging.getLogger(__name__)
PIPELINE_REVISION = "ticks-rollup-v1"


def source_table(config: HistoricalJobConfig) -> str:
    """Return ``databricks_ticks.py``'s owned gold table identifier."""
    layer, name = TICKS_TABLES["summary"]
    return f"{config.catalog}.{config.schema_prefix}_{layer}.{name}"


def read_source(
    spark: SparkSession, config: HistoricalJobConfig
) -> tuple[DataFrame, int]:
    """Read ``databricks_ticks.py``'s gold table at its current pinned version.

    Args:
        spark: Runtime session.
        config: Identifies the source table.

    Returns:
        The source rows (only the columns this transform needs), and the
        exact version read.
    """
    table = source_table(config)
    source_version = table_version(spark, table)
    df = (
        spark.read.option("versionAsOf", source_version)
        .table(table)
        .select(
            "symbol",
            "capture_date_utc",
            "first_observed_price",
            "highest_observed_price",
            "lowest_observed_price",
            "last_observed_price",
            "last_reported_volume",
        )
    )
    return df, source_version


def append_bronze_from_frame(
    spark: SparkSession, config: HistoricalJobConfig, frame: DataFrame
) -> int:
    """Append an already-built OHLCV-shaped frame to bronze; return new version.

    Unlike ``databricks_historical.py::append_bronze`` (which builds a
    DataFrame from a plain ``list[dict]`` fetched via yfinance), this job's
    rows already start as a Spark DataFrame (read from
    ``daily_quote_summary``), so it appends that DataFrame directly instead
    of round-tripping through Python dicts.

    Args:
        spark: Runtime session.
        config: Table identities (shared with databricks_historical.py).
        frame: Rows shaped like BRONZE_SCHEMA, minus extraction_id/
            bronze_ingested_at (added here).

    Returns:
        The bronze table's Delta version after this append.
    """
    # Delta writes match an existing table's columns by name, not position,
    # so no explicit column reordering is needed here - `frame` already
    # carries exactly BRONZE_SCHEMA's column names (see
    # project_quote_summary_as_ohlcv), just adding the two lineage columns
    # every bronze row needs.
    extraction_id = (
        f"ticks-rollup-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    )
    tagged = frame.withColumn("extraction_id", F.lit(extraction_id)).withColumn(
        "bronze_ingested_at", F.current_timestamp()
    )
    (
        tagged.write.format("delta")
        .mode("append")
        .option("path", config.path("raw"))
        .options(**DELTA_AUTO_OPTIMIZE_PROPERTIES)
        .saveAsTable(config.table("raw"))
    )
    return table_version(spark, config.table("raw"))


def rebuild_outputs(spark: SparkSession, config: HistoricalJobConfig) -> dict[str, int]:
    """Roll up the latest daily_quote_summary into historical_ohlcv.

    Args:
        spark: Runtime session; UTC must be configured by the caller.
        config: Table identities and full-rebuild size guard (shared with
            databricks_historical.py - the same tables, the same guard).

    Returns:
        Counts of rolled-up rows this run and the resulting gold row count.

    Raises:
        RuntimeError: If the source table has no rows at all - never
            appends an empty bronze batch silently as if it were a
            legitimate observation.
        ValueError: When the accumulated bronze history exceeds the
            full-rebuild size guard (shared with databricks_historical.py).
    """
    state = ["processing", PIPELINE_REVISION, -1, 0, 0, 0, -1]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")

    source, source_version = read_source(spark, config)
    rolled_up = project_quote_summary_as_ohlcv(source)
    rolled_up_rows = rolled_up.count()
    if rolled_up_rows == 0:
        raise RuntimeError(
            f"daily_quote_summary (version {source_version}) has no rows; "
            "refusing to append an empty bronze batch"
        )

    bronze_version = append_bronze_from_frame(spark, config, rolled_up)
    bronze = spark.read.option("versionAsOf", bronze_version).table(config.table("raw"))
    total_rows = bronze.limit(config.max_input_rows + 1).count()
    if total_rows > config.max_input_rows:
        raise ValueError(
            "Accumulated bronze history exceeds the full-rebuild row limit; "
            "incremental design required"
        )
    latest = rank_latest_per_symbol_date(bronze)
    gold = project_historical(latest).withColumn(
        "bronze_version", F.lit(bronze_version)
    )
    write_snapshot(gold, config, "summary")
    gold_rows = gold.count()
    counts = {
        "rolled_up": rolled_up_rows,
        "gold_rows": gold_rows,
    }
    state = [
        "completed",
        PIPELINE_REVISION,
        bronze_version,
        counts["rolled_up"],
        0,
        gold_rows,
        table_version(spark, config.table("summary")),
    ]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    logger.info("Completed bronze_version=%s counts=%s", bronze_version, counts)
    return counts


def run(spark: SparkSession, config: HistoricalJobConfig) -> dict[str, int]:
    """Roll up - every run is a new sync from the latest daily_quote_summary."""
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
    config = HistoricalJobConfig(
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
