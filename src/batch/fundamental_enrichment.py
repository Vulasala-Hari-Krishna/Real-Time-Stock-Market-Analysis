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
import requests
import yfinance as yf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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


# ------------------------------------------------------------------
# yfinance edge — stays pandas
# ------------------------------------------------------------------


def _fetch_via_api(symbol: str) -> dict[str, Any]:
    """Fetch fundamental data directly from Yahoo Finance API.

    Uses the quoteSummary endpoint with crumb authentication as a
    fallback when yfinance is broken.

    Args:
        symbol: Ticker symbol.

    Returns:
        Dict mapping our field names to values. Missing fields are None.
    """
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    # Get consent cookie
    session.get("https://fc.yahoo.com/", timeout=15)
    # Get crumb
    crumb = session.get(
        "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=15
    ).text

    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        f"?modules=defaultKeyStatistics,summaryDetail,assetProfile"
        f"&crumb={crumb}"
    )
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    result_data = data.get("quoteSummary", {}).get("result")
    if not result_data:
        logger.warning("No quoteSummary result for %s", symbol)
        return {}

    modules = result_data[0]
    stats = modules.get("defaultKeyStatistics", {})
    detail = modules.get("summaryDetail", {})
    profile = modules.get("assetProfile", {})

    def _raw(d: dict, key: str) -> Any:
        """Extract raw value from Yahoo Finance API nested dict."""
        v = d.get(key, {})
        if isinstance(v, dict):
            return v.get("raw")
        return v

    return {
        "market_cap": _raw(detail, "marketCap"),
        "pe_ratio": _raw(detail, "trailingPE"),
        "forward_pe": _raw(stats, "forwardPE"),
        "dividend_yield": _raw(detail, "dividendYield"),
        "eps": _raw(stats, "trailingEps") or _raw(detail, "trailingEps"),
        "beta": _raw(stats, "beta"),
        "fifty_two_week_high": _raw(detail, "fiftyTwoWeekHigh"),
        "fifty_two_week_low": _raw(detail, "fiftyTwoWeekLow"),
        "yf_sector": profile.get("sector"),
        "industry": profile.get("industry"),
    }


def fetch_fundamentals(symbol: str) -> dict[str, Any]:
    """Fetch fundamental data for a single symbol.

    Tries yfinance first. If all fields are empty, falls back to
    the Yahoo Finance quoteSummary API directly.

    Args:
        symbol: Ticker symbol (e.g. "AAPL").

    Returns:
        Dict with standardised fundamental field names. Missing fields
        are set to None.
    """
    logger.info("Fetching fundamentals for %s", symbol)
    result: dict[str, Any] = {"symbol": symbol.upper()}

    # Try yfinance first
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
        for yf_key, our_key in FUNDAMENTAL_FIELDS.items():
            result[our_key] = info.get(yf_key)
    except Exception:
        logger.warning("yfinance raised an error for %s", symbol, exc_info=True)

    # Check if yfinance returned anything useful
    value_keys = [k for k in result if k not in ("symbol", "retrieved_at")]
    has_data = any(result.get(k) is not None for k in value_keys)

    if not has_data:
        logger.warning("yfinance returned empty info for %s, trying direct API", symbol)
        try:
            api_data = _fetch_via_api(symbol)
            for k, v in api_data.items():
                result[k] = v
        except Exception:
            logger.exception("Direct API fallback also failed for %s", symbol)

    result["retrieved_at"] = datetime.now(timezone.utc).isoformat()
    populated = sum(1 for k in value_keys if result.get(k) is not None)
    logger.info(
        "Fetched fundamentals for %s: %d fields populated",
        symbol,
        populated,
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

    # Cast numeric columns to float64 (yfinance/API may return ints
    # which PySpark's DoubleType rejects)
    for field in FUNDAMENTALS_SCHEMA.fields:
        if isinstance(field.dataType, DoubleType):
            pdf[field.name] = pd.to_numeric(pdf[field.name], errors="coerce").astype(
                "float64"
            )

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
    dropped from the fundamentals side before joining.

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


def write_fundamentals_gold(spark: SparkSession, df: DataFrame, gold_path: str) -> None:
    """Write standalone fundamentals to the gold layer as Delta.

    Uses MERGE (upsert by symbol) if the Delta table already exists,
    otherwise writes a new Delta table.

    Args:
        spark: Active SparkSession.
        df: Spark DataFrame of fundamentals.
        gold_path: S3a base path for gold layer.
    """
    from delta.tables import DeltaTable

    path = f"{gold_path}/fundamentals"
    try:
        if DeltaTable.isDeltaTable(spark, path):
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.alias("existing").merge(
                df.alias("new"),
                "existing.symbol = new.symbol",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info("Merged fundamentals into %s (Delta MERGE)", path)
            return
    except Exception:
        pass
    df.write.format("delta").mode("overwrite").save(path)
    logger.info("Wrote gold/fundamentals to %s (Delta overwrite)", path)


def write_enriched_gold(spark: SparkSession, df: DataFrame, gold_path: str) -> None:
    """Write enriched price+fundamentals to the gold layer as Delta.

    Uses MERGE (upsert by symbol + date) if the Delta table already
    exists, otherwise writes a new Delta table.

    Args:
        spark: Active SparkSession.
        df: Enriched Spark DataFrame.
        gold_path: S3a base path for gold layer.
    """
    from delta.tables import DeltaTable

    path = f"{gold_path}/enriched_prices"
    try:
        if DeltaTable.isDeltaTable(spark, path):
            max_date = df.agg(F.max("date")).collect()[0][0]
            new_rows = df.filter(F.col("date") == max_date)
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.alias("existing").merge(
                new_rows.alias("new"),
                "existing.symbol = new.symbol AND existing.date = new.date",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info("Merged enriched_prices into %s (Delta MERGE)", path)
            return
    except Exception:
        pass
    df.write.format("delta").mode("overwrite").save(path)
    logger.info("Wrote gold/enriched_prices to %s (Delta overwrite)", path)


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
        write_fundamentals_gold(spark, outputs["fundamentals"], gold_path)
        write_enriched_gold(spark, outputs["enriched"], gold_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
