"""Fundamental data enrichment batch job using PySpark.

Fetches company fundamental data (P/E ratio, market cap, dividend yield,
sector, industry, 52-week high/low, earnings) from yfinance for every
stock in the watchlist. Converts to a Spark DataFrame, joins with
silver-layer price data, and writes the enriched output to the S3 gold
layer as Parquet with snappy compression.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import get_settings
from src.config.watchlist import SYMBOLS

logger = logging.getLogger(__name__)

# Fields to extract from yfinance Ticker.info
FUNDAMENTAL_FIELDS: dict[str, str] = {
    "marketCap": "market_cap",
    "trailingPE": "pe_ratio",
    "forwardPE": "forward_pe",
    "dividendYield": "dividend_yield",
    "trailingEps": "eps",
    "beta": "beta",
    "fiftyTwoWeekHigh": "fifty_two_week_high",
    "fiftyTwoWeekLow": "fifty_two_week_low",
    "sector": "yf_sector",
    "industry": "industry",
}

# Explicit Spark schema for the fundamentals DataFrame
FUNDAMENTALS_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("market_cap", DoubleType(), nullable=True),
        StructField("pe_ratio", DoubleType(), nullable=True),
        StructField("forward_pe", DoubleType(), nullable=True),
        StructField("dividend_yield", DoubleType(), nullable=True),
        StructField("eps", DoubleType(), nullable=True),
        StructField("beta", DoubleType(), nullable=True),
        StructField("fifty_two_week_high", DoubleType(), nullable=True),
        StructField("fifty_two_week_low", DoubleType(), nullable=True),
        StructField("yf_sector", StringType(), nullable=True),
        StructField("industry", StringType(), nullable=True),
        StructField("retrieved_at", StringType(), nullable=True),
    ]
)


def create_spark_session(
    app_name: str = "FundamentalEnrichment",
) -> SparkSession:
    """Create a SparkSession for the enrichment job.

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    settings = get_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
    )

    if settings.aws_access_key_id:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key
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


# ------------------------------------------------------------------
# yfinance edge — stays pandas
# ------------------------------------------------------------------


def fetch_fundamentals(symbol: str) -> dict[str, Any]:
    """Fetch fundamental data for a single symbol from yfinance.

    Args:
        symbol: Ticker symbol (e.g. "AAPL").

    Returns:
        Dict with standardised fundamental field names. Missing fields
        are set to None.
    """
    logger.info("Fetching fundamentals for %s", symbol)
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}

    result: dict[str, Any] = {"symbol": symbol.upper()}
    for yf_key, our_key in FUNDAMENTAL_FIELDS.items():
        result[our_key] = info.get(yf_key)

    result["retrieved_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "Fetched fundamentals for %s: %d fields populated",
        symbol,
        sum(1 for v in result.values() if v is not None) - 2,
    )
    return result


def fetch_all_fundamentals(symbols: list[str]) -> list[dict[str, Any]]:
    """Fetch fundamentals for multiple symbols.

    Args:
        symbols: List of ticker symbols.

    Returns:
        List of dicts (one per symbol). Convert to Spark downstream.
    """
    records: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            record = fetch_fundamentals(symbol)
            records.append(record)
        except Exception:
            logger.exception("Failed to fetch fundamentals for %s", symbol)
            records.append(
                {
                    "symbol": symbol.upper(),
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return records


def fundamentals_to_spark(
    spark: SparkSession, records: list[dict[str, Any]]
) -> DataFrame:
    """Convert fetched fundamentals to a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        records: List of dicts from fetch_all_fundamentals.

    Returns:
        Spark DataFrame with FUNDAMENTALS_SCHEMA.
    """
    if not records:
        return spark.createDataFrame([], FUNDAMENTALS_SCHEMA)

    pdf = pd.DataFrame(records)
    # Ensure all schema columns exist
    for field in FUNDAMENTALS_SCHEMA.fields:
        if field.name not in pdf.columns:
            pdf[field.name] = None
    pdf = pdf[[f.name for f in FUNDAMENTALS_SCHEMA.fields]]

    # Cast numeric columns to float (yfinance may return ints)
    for field in FUNDAMENTALS_SCHEMA.fields:
        if isinstance(field.dataType, DoubleType):
            pdf[field.name] = pd.to_numeric(pdf[field.name], errors="coerce")

    return spark.createDataFrame(pdf, schema=FUNDAMENTALS_SCHEMA)


# ------------------------------------------------------------------
# Spark join
# ------------------------------------------------------------------


def enrich_with_fundamentals(
    price_df: DataFrame, fundamentals_df: DataFrame
) -> DataFrame:
    """Join price data with fundamental data on symbol.

    Uses a left join so every price row is preserved. Columns that
    overlap between price and fundamentals (except ``symbol``) are
    dropped from the fundamentals side before joini.

    Args:
        price_df: Silver-layer OHLCV Spark DataFrame.
        fundamentals_df: Spark DataFrame of fundamentals.

    Returns:
        Enriched Spark DataFrame.
    """
    price_cols = set(price_df.columns)
    fund_cols = set(fundamentals_df.columns)
    overlap = (price_cols & fund_cols) - {"symbol"}

    if overlap:
        fundamentals_df = fundamentals_df.drop(*overlap)

    enriched = price_df.join(fundamentals_df, on="symbol", how="left")
    logger.info("Enriched price data with fundamentals via Spark join")
    return enriched


# ------------------------------------------------------------------
# Gold layer writers
# ------------------------------------------------------------------


def write_fundamentals_gold(df: DataFrame, gold_path: str) -> None:
    """Write standalone fundamentals to the gold layer.

    Args:
        df: Spark DataFrame of fundamentals.
        gold_path: S3a base path for gold layer.
    """
    path = f"{gold_path}/fundamentals"
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    logger.info("Wrote gold/fundamentals to %s", path)


def write_enriched_gold(df: DataFrame, gold_path: str) -> None:
    """Write enriched price+fundamentals to the gold layer.

    Args:
        df: Enriched Spark DataFrame.
        gold_path: S3a base path for gold layer.
    """
    path = f"{gold_path}/enriched_prices"
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    logger.info("Wrote gold/enriched_prices to %s", path)


# ------------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------------


def run_fundamental_enrichment(
    spark: SparkSession,
    price_df: DataFrame,
    symbols: list[str] | None = None,
) -> dict[str, DataFrame]:
    """Run the full fundamental enrichment pipeline.

    Args:
        spark: Active SparkSession.
        price_df: Silver-layer OHLCV Spark DataFrame.
        symbols: Symbols to enrich. Defaults to SYMBOLS.

    Returns:
        Dict with 'fundamentals' and 'enriched' Spark DataFrames.
    """
    symbols = symbols or SYMBOLS

    records = fetch_all_fundamentals(symbols)
    fundamentals_df = fundamentals_to_spark(spark, records)
    enriched_df = enrich_with_fundamentals(price_df, fundamentals_df)

    return {
        "fundamentals": fundamentals_df,
        "enriched": enriched_df,
    }


def main() -> None:
    """Entry point for the fundamental enrichment batch job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = get_settings()
    spark = create_spark_session()

    try:
        bucket = settings.s3_bucket_name
        silver_path = f"s3a://{bucket}/silver/historical"
        gold_path = f"s3a://{bucket}/gold"

        price_df = spark.read.parquet(silver_path)
        outputs = run_fundamental_enrichment(spark, price_df)
        write_fundamentals_gold(outputs["fundamentals"], gold_path)
        write_enriched_gold(outputs["enriched"], gold_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
