"""Historical OHLCV backfill using yfinance and PySpark.

Downloads 5 years of daily OHLCV data for every stock in the watchlist
via yfinance (pandas at the edge), immediately converts to a Spark
DataFrame, writes raw JSON to the S3 bronze layer, then transforms and
writes Parquet to the silver layer partitioned by year/month.

Idempotent: safe to re-run — existing partitions are overwritten.
"""

import logging
import time

import pandas as pd
import requests as _requests
import yfinance as yf
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
from src.config.watchlist import SYMBOLS

logger = logging.getLogger(__name__)

BACKFILL_YEARS = 5

# Explicit schema for the silver layer
OHLCV_SCHEMA = StructType(
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


def create_spark_session(app_name: str = "HistoricalBackfill") -> SparkSession:
    """Create and configure a SparkSession for batch processing.

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


def download_history(symbol: str, years: int = BACKFILL_YEARS) -> pd.DataFrame:
    """Download historical OHLCV data from Yahoo Finance.

    Uses the Yahoo Finance chart API directly as a primary method,
    falling back to yfinance if the direct call fails.

    Args:
        symbol: Ticker symbol (e.g. "AAPL").
        years: Number of years of history to fetch.

    Returns:
        pandas DataFrame with date, open, high, low, close, volume, symbol.
        Empty DataFrame if the download fails or returns no data.
    """
    period = f"{years}y"
    logger.info("Downloading %s history for %s", period, symbol)

    df = _download_via_api(symbol, period)
    if df is None or df.empty:
        logger.info("Direct API failed for %s, trying yfinance fallback", symbol)
        df = yf.download(symbol, period=period, auto_adjust=True, progress=False)

    if df is None or df.empty:
        logger.warning("No data returned for %s", symbol)
        return pd.DataFrame()

    df = df.reset_index()

    # Normalise column names to lowercase
    df.columns = [c.lower() if isinstance(c, str) else c for c in df.columns]

    # Handle MultiIndex columns from yfinance (symbol as second level)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() if isinstance(c, tuple) else c for c in df.columns]

    expected = {"date", "open", "high", "low", "close", "volume"}
    if not expected.issubset(set(df.columns)):
        logger.warning("Unexpected columns for %s: %s", symbol, list(df.columns))
        return pd.DataFrame()

    df["symbol"] = symbol.upper()
    logger.info("Downloaded %d rows for %s", len(df), symbol)
    return df


def _download_via_api(symbol: str, period: str) -> pd.DataFrame:
    """Download OHLCV data directly from Yahoo Finance chart API.

    Args:
        symbol: Ticker symbol.
        period: Period string (e.g. "5y").

    Returns:
        pandas DataFrame or empty DataFrame on failure.
    """
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": period, "interval": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = _requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        quotes = result["indicators"]["quote"][0]
        return pd.DataFrame(
            {
                "date": pd.to_datetime(timestamps, unit="s"),
                "open": quotes["open"],
                "high": quotes["high"],
                "low": quotes["low"],
                "close": quotes["close"],
                "volume": quotes["volume"],
            }
        )
    except Exception:
        logger.warning("Direct Yahoo API call failed for %s", symbol)
        return pd.DataFrame()


def pandas_to_spark(spark: SparkSession, pdf: pd.DataFrame) -> DataFrame:
    """Convert a pandas OHLCV DataFrame to a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        pdf: pandas DataFrame from download_history.

    Returns:
        Spark DataFrame with OHLCV_SCHEMA columns.
    """
    # Select and cast columns to match our schema before conversion
    pdf = pdf[["symbol", "date", "open", "high", "low", "close", "volume"]].copy()
    pdf["date"] = pd.to_datetime(pdf["date"], utc=True)
    pdf["open"] = pdf["open"].astype(float)
    pdf["high"] = pdf["high"].astype(float)
    pdf["low"] = pdf["low"].astype(float)
    pdf["close"] = pdf["close"].astype(float)
    pdf["volume"] = pdf["volume"].astype("int64")
    pdf["source"] = "yfinance"

    return spark.createDataFrame(pdf, schema=OHLCV_SCHEMA)


def write_bronze(df: DataFrame, output_path: str) -> None:
    """Write raw data as JSON to the S3 bronze layer, partitioned.

    Args:
        df: Spark DataFrame with symbol and date columns.
        output_path: S3a output path (e.g. ``s3a://bucket/bronze/historical``).
    """
    (
        df.withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.date_format(F.col("date"), "MM"))
        .write.mode("overwrite")
        .partitionBy("symbol", "year", "month")
        .json(output_path)
    )
    logger.info("Wrote bronze JSON to %s", output_path)


def transform_to_silver(df: DataFrame) -> DataFrame:
    """Transform raw data to the silver schema.

    Casts types, adds partition columns, and drops duplicates.

    Args:
        df: Raw OHLCV Spark DataFrame.

    Returns:
        Cleaned silver-layer Spark DataFrame.
    """
    return (
        df.dropDuplicates(["symbol", "date"])
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.date_format(F.col("date"), "MM"))
    )


def write_silver(df: DataFrame, output_path: str) -> None:
    """Write silver data as Parquet, partitioned by year/month.

    Args:
        df: Silver Spark DataFrame with year/month partition columns.
        output_path: S3a output path (e.g. ``s3a://bucket/silver/historical``).
    """
    (
        df.write.mode("overwrite")
        .partitionBy("symbol", "year", "month")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info("Wrote silver Parquet to %s", output_path)


def backfill_symbol(
    spark: SparkSession, symbol: str, bronze_path: str, silver_path: str
) -> bool:
    """Run the full backfill pipeline for a single symbol.

    Args:
        spark: Active SparkSession.
        symbol: Ticker symbol.
        bronze_path: S3a path for bronze output.
        silver_path: S3a path for silver output.

    Returns:
        True if download, transform, and write succeeded.
    """
    pdf = download_history(symbol)
    if pdf.empty:
        logger.warning("Skipping %s — no data downloaded", symbol)
        return False

    sdf = pandas_to_spark(spark, pdf)
    write_bronze(sdf, bronze_path)
    silver_df = transform_to_silver(sdf)
    write_silver(silver_df, silver_path)

    logger.info("Backfill complete for %s", symbol)
    return True


def run_backfill(
    spark: SparkSession | None = None,
    symbols: list[str] | None = None,
) -> dict[str, bool]:
    """Run historical backfill for all (or specified) symbols.

    Args:
        spark: SparkSession. Created if not provided.
        symbols: List of ticker symbols. Defaults to the full watchlist.

    Returns:
        Dict mapping each symbol to its success status.
    """
    settings = get_settings()
    symbols = symbols or SYMBOLS

    if spark is None:
        spark = create_spark_session()

    bucket = settings.s3_bucket_name
    bronze_path = f"s3a://{bucket}/bronze/historical"
    silver_path = f"s3a://{bucket}/silver/historical"

    results: dict[str, bool] = {}

    logger.info(
        "Starting historical backfill: %d symbols, bucket=%s",
        len(symbols),
        bucket,
    )

    for i, symbol in enumerate(symbols):
        if i > 0:
            time.sleep(2)  # Rate-limit yfinance requests
        try:
            results[symbol] = backfill_symbol(spark, symbol, bronze_path, silver_path)
        except Exception:
            logger.exception("Backfill failed for %s", symbol)
            results[symbol] = False

    succeeded = sum(1 for v in results.values() if v)
    failed = len(results) - succeeded
    logger.info(
        "Backfill complete: %d succeeded, %d failed out of %d",
        succeeded,
        failed,
        len(results),
    )
    return results


def main() -> None:
    """Entry point for the historical backfill job."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    spark = create_spark_session()
    try:
        results = run_backfill(spark)
        succeeded = sum(1 for v in results.values() if v)
        if succeeded == 0:
            logger.error("All symbols failed — exiting with error")
            sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
