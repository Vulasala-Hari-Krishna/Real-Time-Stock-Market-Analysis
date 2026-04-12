"""Daily tick-to-OHLCV rollup using PySpark.

Reads the real-time tick data written by Spark Streaming to the
``silver/stock_ticks`` path, aggregates into daily OHLCV bars per
symbol, and appends them to ``silver/historical`` so the downstream
``daily_aggregation`` batch job can compute indicators on fresh data.

Optimised with **partition pruning**: when a ``--date`` argument is
provided (default: yesterday) the job reads only the single day-level
partition (``year=YYYY/month=MM/day=DD``) instead of the entire
stock_ticks dataset.  Use ``--date all`` to fall back to the original
full-scan behaviour.

Idempotent: uses a ``left_anti`` join on (symbol, date) against the
existing silver/historical data, so re-runs for the same day are safe.
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Output schema — must match the silver/historical OHLCV schema
# used by historical_backfill and daily_aggregation.
DAILY_OHLCV_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("date", TimestampType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("source", StringType(), nullable=True),
    ]
)


def create_spark_session(app_name: str = "DailyTickRollup") -> SparkSession:
    """Create a SparkSession for the tick rollup job.

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    settings = get_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if settings.aws_access_key_id:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
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


def _partition_path(base_path: str, target_date: date) -> str:
    """Build the day-level partition path for a given date.

    Args:
        base_path: S3a base path to silver/stock_ticks.
        target_date: The calendar date to read.

    Returns:
        Full path like ``s3a://…/silver/stock_ticks/year=2026/month=04/day=12``.
    """
    return (
        f"{base_path}"
        f"/year={target_date.year}"
        f"/month={target_date.month:02d}"
        f"/day={target_date.day:02d}"
    )


def read_tick_data(
    spark: SparkSession, ticks_path: str, target_date: date | None = None
) -> DataFrame:
    """Read real-time tick Parquet data from the silver layer.

    When *target_date* is provided the reader uses partition pruning,
    reading only ``year=YYYY/month=MM/day=DD`` under *ticks_path*.
    When ``None`` the full dataset is read (legacy behaviour).

    Args:
        spark: Active SparkSession.
        ticks_path: S3a path to silver/stock_ticks.
        target_date: Optional calendar date for partition pruning.

    Returns:
        Spark DataFrame with tick-level data, or empty DataFrame
        if the path does not exist yet (first-run scenario).
    """
    read_path = _partition_path(ticks_path, target_date) if target_date else ticks_path
    try:
        df = spark.read.parquet(read_path)
        logger.info(
            "Read %d tick partitions from %s", df.rdd.getNumPartitions(), read_path
        )
        return df
    except Exception:
        logger.warning(
            "No tick data found at %s (first run?). Returning empty DataFrame.",
            read_path,
        )
        return spark.createDataFrame([], DAILY_OHLCV_SCHEMA)


def rollup_ticks_to_daily(df: DataFrame) -> DataFrame:
    """Aggregate raw ticks into daily OHLCV bars per symbol.

    Groups by symbol and calendar date, computing:
    - open: first price of the day (by timestamp)
    - high: max price
    - low: min price
    - close: last price of the day (by timestamp)
    - volume: sum of all tick volumes

    Args:
        df: Tick-level DataFrame with symbol, price, volume, timestamp.

    Returns:
        DataFrame with one row per (symbol, date) containing OHLCV columns.
    """
    if df.isEmpty():
        return df

    daily = (
        df.withColumn("date", F.to_date(F.col("timestamp")))
        .groupBy("symbol", "date")
        .agg(
            F.first("price", ignorenulls=True).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price", ignorenulls=True).alias("close"),
            F.sum("volume").alias("volume"),
        )
        .withColumn("source", F.lit("realtime"))
        .withColumn("date", F.col("date").cast(TimestampType()))
    )

    return daily


def deduplicate_against_existing(
    new_df: DataFrame,
    existing_path: str,
    spark: SparkSession,
    target_date: date | None = None,
) -> DataFrame:
    """Remove rows from new_df that already exist in the historical data.

    This makes the job idempotent — re-running for the same day won't
    create duplicate rows.

    When *target_date* is provided, only the matching ``year``/``month``
    partitions are read from the historical data (partition pruning).
    For a 5-year, 10-symbol history this reduces the scan from ~600
    partitions to ~10 (one per symbol for that month).

    Args:
        new_df: Newly rolled-up daily OHLCV bars.
        existing_path: S3a path to silver/historical.
        spark: Active SparkSession.
        target_date: Optional date used to prune historical partitions.

    Returns:
        DataFrame containing only rows not already in the historical data.
    """
    if new_df.isEmpty():
        return new_df

    try:
        existing = spark.read.parquet(existing_path)

        if target_date is not None:
            existing = existing.filter(
                (F.col("year") == target_date.year)
                & (F.col("month") == f"{target_date.month:02d}")
            )
            logger.info(
                "Dedup partition pruning: reading only year=%d/month=%02d",
                target_date.year,
                target_date.month,
            )

        existing_keys = existing.select("symbol", "date").distinct()
        new_with_flag = new_df.join(
            existing_keys, on=["symbol", "date"], how="left_anti"
        )
        new_count = new_with_flag.count()
        logger.info(
            "Deduplication: %d new bars after filtering existing data",
            new_count,
        )
        return new_with_flag
    except Exception:
        logger.info(
            "No existing historical data at %s (first run). " "All %d bars are new.",
            existing_path,
            new_df.count(),
        )
        return new_df


def write_daily_bars(df: DataFrame, output_path: str) -> int:
    """Append daily OHLCV bars to the silver/historical layer.

    Args:
        df: Daily OHLCV DataFrame to write.
        output_path: S3a path to silver/historical.

    Returns:
        Number of rows written.
    """
    if df.isEmpty():
        logger.info("No new bars to write.")
        return 0

    partitioned = df.withColumn("year", F.year(F.col("date"))).withColumn(
        "month", F.date_format(F.col("date"), "MM")
    )

    (
        partitioned.write.mode("append")
        .partitionBy("symbol", "year", "month")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    row_count = df.count()
    logger.info("Wrote %d daily bars to %s", row_count, output_path)
    return row_count


def _parse_target_date(value: str | None) -> date | None:
    """Convert the CLI *--date* value to a :class:`date` or ``None``.

    * ``None`` / ``"yesterday"`` → yesterday's date (default).
    * ``"all"`` → ``None`` (full-scan, legacy behaviour).
    * ``"YYYY-MM-DD"`` → explicit date.
    """
    if value is None or value.lower() == "yesterday":
        return datetime.now(timezone.utc).date() - timedelta(days=1)
    if value.lower() == "all":
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def run_tick_rollup(
    spark: SparkSession | None = None, target_date_str: str | None = None
) -> int:
    """Run the full tick-to-OHLCV rollup pipeline.

    Args:
        spark: SparkSession. Created if not provided.
        target_date_str: Date string controlling partition pruning.
            ``None`` / ``"yesterday"`` → read only yesterday's partition.
            ``"all"`` → read entire stock_ticks (legacy full-scan).
            ``"YYYY-MM-DD"`` → read that specific day's partition.

    Returns:
        Number of new daily bars written.
    """
    settings = get_settings()

    if spark is None:
        spark = create_spark_session()

    target_date = _parse_target_date(target_date_str)

    bucket = settings.s3_bucket_name
    ticks_path = f"s3a://{bucket}/silver/stock_ticks"
    historical_path = f"s3a://{bucket}/silver/historical"

    logger.info(
        "Starting tick rollup: bucket=%s, target_date=%s",
        bucket,
        target_date or "ALL",
    )

    tick_df = read_tick_data(spark, ticks_path, target_date)
    daily_df = rollup_ticks_to_daily(tick_df)
    deduped_df = deduplicate_against_existing(
        daily_df, historical_path, spark, target_date
    )
    rows_written = write_daily_bars(deduped_df, historical_path)

    logger.info("Tick rollup complete: %d new bars written", rows_written)
    return rows_written


def main() -> None:
    """Entry point for the daily tick rollup job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Daily tick-to-OHLCV rollup")
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Target date for partition pruning. "
            "'yesterday' (default) reads only yesterday's ticks. "
            "'all' reads the entire stock_ticks dataset. "
            "'YYYY-MM-DD' reads a specific day."
        ),
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run_tick_rollup(spark, target_date_str=args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
