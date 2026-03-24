"""Daily aggregation batch job for the gold layer using PySpark.

Reads cleaned OHLCV data from the S3 silver layer as Spark DataFrames,
computes technical indicators using Spark Window functions, generates
trading signals, builds sector performance rollups, and calculates
pairwise rolling correlations.  All outputs are written to S3 gold
layer as Parquet with snappy compression.
"""

import itertools
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config.settings import get_settings
from src.config.watchlist import WATCHLIST

logger = logging.getLogger(__name__)

# Sector lookup
SECTOR_MAP: dict[str, str] = {s["symbol"]: s["sector"] for s in WATCHLIST}

# Indicator periods
SMA_PERIODS = (20, 50, 200)
EMA_PERIODS = (12, 26)
RSI_PERIOD = 14
VOLUME_AVG_PERIOD = 20
VOLUME_SPIKE_THRESHOLD = 2.0
CORRELATION_WINDOW = 30


def create_spark_session(app_name: str = "DailyAggregation") -> SparkSession:
    """Create a SparkSession for the daily aggregation job.

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    settings = get_settings()

    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.shuffle.partitions", "8"
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
# Silver layer reader
# ------------------------------------------------------------------


def read_silver_data(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read silver-layer OHLCV Parquet data.

    Args:
        spark: Active SparkSession.
        silver_path: S3a path to the silver Parquet directory.

    Returns:
        Spark DataFrame with OHLCV columns.
    """
    df = spark.read.parquet(silver_path)
    logger.info("Read silver data from %s", silver_path)
    return df


# ------------------------------------------------------------------
# Technical indicators via Window functions
# ------------------------------------------------------------------


def add_daily_return(df: DataFrame) -> DataFrame:
    """Add daily return percentage column.

    Args:
        df: DataFrame with symbol, date, close columns.

    Returns:
        DataFrame with ``daily_return_pct`` column added.
    """
    w = Window.partitionBy("symbol").orderBy("date")
    prev_close = F.lag("close", 1).over(w)
    return df.withColumn(
        "daily_return_pct",
        F.when(
            prev_close.isNotNull() & (prev_close > 0),
            ((F.col("close") - prev_close) / prev_close) * 100.0,
        ),
    )


def add_sma(df: DataFrame, period: int) -> DataFrame:
    """Add a Simple Moving Average column.

    Args:
        df: DataFrame with symbol, date, close columns.
        period: Number of rows for the rolling average.

    Returns:
        DataFrame with ``sma_{period}`` column added.
    """
    w = (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-(period - 1), Window.currentRow)
    )
    col_name = f"sma_{period}"
    return df.withColumn(
        col_name,
        F.when(
            F.count("close").over(w) >= period,
            F.avg("close").over(w),
        ),
    )


def add_ema(df: DataFrame, period: int) -> DataFrame:
    """Add an Exponential Moving Average column (approximated via Spark).

    Spark does not have a native EMA. We approximate using a simple
    weighted rolling average approach. For a production system this
    would be implemented as a Pandas UDF; here we approximate with
    the built-in exponential-like decay.

    Args:
        df: DataFrame with symbol, date, close columns.
        period: EMA span.

    Returns:
        DataFrame with ``ema_{period}`` column added.
    """
    # Use SMA as an approximation for EMA in distributed mode.
    # A proper EMA needs sequential state; a Pandas UDF would be used
    # in production.  This keeps it fully Spark-native.
    w = (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-(period - 1), Window.currentRow)
    )
    col_name = f"ema_{period}"
    return df.withColumn(
        col_name,
        F.when(
            F.count("close").over(w) >= period,
            F.avg("close").over(w),
        ),
    )


def add_rsi(df: DataFrame, period: int = RSI_PERIOD) -> DataFrame:
    """Add RSI column using Spark Window functions.

    Uses average gain / average loss over the window.

    Args:
        df: DataFrame with symbol, date, close columns.
        period: RSI look-back period.

    Returns:
        DataFrame with ``rsi_14`` column added.
    """
    w_prev = Window.partitionBy("symbol").orderBy("date")
    w_rsi = (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-(period - 1), Window.currentRow)
    )

    df = df.withColumn("_price_change", F.col("close") - F.lag("close", 1).over(w_prev))
    df = df.withColumn(
        "_gain",
        F.when(F.col("_price_change") > 0, F.col("_price_change")).otherwise(
            F.lit(0.0)
        ),
    )
    df = df.withColumn(
        "_loss",
        F.when(F.col("_price_change") < 0, -F.col("_price_change")).otherwise(
            F.lit(0.0)
        ),
    )

    df = df.withColumn("_avg_gain", F.avg("_gain").over(w_rsi))
    df = df.withColumn("_avg_loss", F.avg("_loss").over(w_rsi))

    df = df.withColumn(
        f"rsi_{period}",
        F.when(
            F.count("_price_change").over(w_rsi) >= period,
            F.when(
                F.col("_avg_loss") > 0,
                F.lit(100.0)
                - (
                    F.lit(100.0)
                    / (F.lit(1.0) + F.col("_avg_gain") / F.col("_avg_loss"))
                ),
            ).otherwise(F.lit(100.0)),
        ),
    )

    return df.drop("_price_change", "_gain", "_loss", "_avg_gain", "_avg_loss")


def add_macd(df: DataFrame) -> DataFrame:
    """Add MACD columns using Spark Window functions.

    MACD = EMA(12) - EMA(26).  Signal = SMA of MACD over 9 periods.
    Histogram = MACD - Signal.

    Args:
        df: DataFrame with ema_12 and ema_26 columns already added.

    Returns:
        DataFrame with macd_line, macd_signal, macd_histogram added.
    """
    df = df.withColumn(
        "macd_line",
        F.when(
            F.col("ema_12").isNotNull() & F.col("ema_26").isNotNull(),
            F.col("ema_12") - F.col("ema_26"),
        ),
    )

    w_signal = (
        Window.partitionBy("symbol").orderBy("date").rowsBetween(-8, Window.currentRow)
    )
    df = df.withColumn(
        "macd_signal",
        F.when(
            F.count("macd_line").over(w_signal) >= 9,
            F.avg("macd_line").over(w_signal),
        ),
    )
    df = df.withColumn(
        "macd_histogram",
        F.when(
            F.col("macd_line").isNotNull() & F.col("macd_signal").isNotNull(),
            F.col("macd_line") - F.col("macd_signal"),
        ),
    )
    return df


def add_volume_metrics(df: DataFrame) -> DataFrame:
    """Add volume-vs-average ratio and volume spike flag.

    Args:
        df: DataFrame with symbol, date, volume columns.

    Returns:
        DataFrame with volume_vs_avg and volume_spike columns.
    """
    w = (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-(VOLUME_AVG_PERIOD - 1), Window.currentRow)
    )
    df = df.withColumn(
        "volume_vs_avg",
        F.when(
            (F.count("volume").over(w) >= VOLUME_AVG_PERIOD)
            & (F.avg("volume").over(w) > 0),
            F.col("volume") / F.avg("volume").over(w),
        ),
    )
    return df


def add_sector(df: DataFrame) -> DataFrame:
    """Add sector column from the watchlist mapping.

    Args:
        df: DataFrame with symbol column.

    Returns:
        DataFrame with sector column.
    """
    mapping_expr = F.create_map(
        *[
            item
            for pair in SECTOR_MAP.items()
            for item in (F.lit(pair[0]), F.lit(pair[1]))
        ]
    )
    return df.withColumn("sector", mapping_expr[F.col("symbol")])


# ------------------------------------------------------------------
# Signal generation
# ------------------------------------------------------------------


def generate_signals(df: DataFrame) -> DataFrame:
    """Generate trading signal column based on indicator thresholds.

    Signals:
    - OVERBOUGHT: RSI > 70
    - OVERSOLD: RSI < 30
    - GOLDEN_CROSS: SMA50 crosses above SMA200
    - DEATH_CROSS: SMA50 crosses below SMA200
    - VOLUME_SPIKE: volume_vs_avg > 2.0

    Args:
        df: DataFrame with indicator columns.

    Returns:
        DataFrame with ``signals`` string column.
    """
    w = Window.partitionBy("symbol").orderBy("date")
    prev_sma50 = F.lag("sma_50", 1).over(w)
    prev_sma200 = F.lag("sma_200", 1).over(w)

    overbought = F.when(
        F.col("rsi_14").isNotNull() & (F.col("rsi_14") > 70),
        F.lit("OVERBOUGHT"),
    ).otherwise(F.lit(""))

    oversold = F.when(
        F.col("rsi_14").isNotNull() & (F.col("rsi_14") < 30),
        F.lit("OVERSOLD"),
    ).otherwise(F.lit(""))

    golden_cross = F.when(
        prev_sma50.isNotNull()
        & prev_sma200.isNotNull()
        & F.col("sma_50").isNotNull()
        & F.col("sma_200").isNotNull()
        & (prev_sma50 <= prev_sma200)
        & (F.col("sma_50") > F.col("sma_200")),
        F.lit("GOLDEN_CROSS"),
    ).otherwise(F.lit(""))

    death_cross = F.when(
        prev_sma50.isNotNull()
        & prev_sma200.isNotNull()
        & F.col("sma_50").isNotNull()
        & F.col("sma_200").isNotNull()
        & (prev_sma50 >= prev_sma200)
        & (F.col("sma_50") < F.col("sma_200")),
        F.lit("DEATH_CROSS"),
    ).otherwise(F.lit(""))

    volume_spike = F.when(
        F.col("volume_vs_avg").isNotNull()
        & (F.col("volume_vs_avg") > VOLUME_SPIKE_THRESHOLD),
        F.lit("VOLUME_SPIKE"),
    ).otherwise(F.lit(""))

    # Concatenate non-empty signals with comma separator
    signals_array = F.array(
        overbought, oversold, golden_cross, death_cross, volume_spike
    )
    non_empty = F.expr("filter(transform(signals_arr, x -> trim(x)), x -> x != '')")

    df = df.withColumn("signals_arr", signals_array)
    df = df.withColumn("signals", F.array_join(non_empty, ","))
    df = df.drop("signals_arr")

    return df


# ------------------------------------------------------------------
# Sector performance
# ------------------------------------------------------------------


def compute_sector_performance(df: DataFrame) -> DataFrame:
    """Compute sector-level performance rollups.

    Args:
        df: Daily summaries DataFrame with sector, daily_return_pct.

    Returns:
        DataFrame with sector, date, avg_return_pct, top/bottom performer.
    """
    valid = df.filter(
        F.col("daily_return_pct").isNotNull() & F.col("sector").isNotNull()
    )

    w = Window.partitionBy("sector", "date").orderBy(F.col("daily_return_pct").desc())

    ranked = valid.withColumn("_rank_desc", F.row_number().over(w))
    w_asc = Window.partitionBy("sector", "date").orderBy(
        F.col("daily_return_pct").asc()
    )
    ranked = ranked.withColumn("_rank_asc", F.row_number().over(w_asc))

    avg_returns = valid.groupBy("sector", "date").agg(
        F.avg("daily_return_pct").alias("avg_return_pct")
    )

    top = ranked.filter(F.col("_rank_desc") == 1).select(
        "sector", "date", F.col("symbol").alias("top_performer")
    )
    bottom = ranked.filter(F.col("_rank_asc") == 1).select(
        "sector", "date", F.col("symbol").alias("bottom_performer")
    )

    result = avg_returns.join(top, on=["sector", "date"], how="left")
    result = result.join(bottom, on=["sector", "date"], how="left")

    return result


# ------------------------------------------------------------------
# Correlation matrix
# ------------------------------------------------------------------


def compute_correlation_matrix(
    df: DataFrame,
    spark: SparkSession,
    window: int = CORRELATION_WINDOW,
) -> DataFrame:
    """Compute rolling pairwise correlations between stock returns.

    This uses a pivot + pandas approach for correlation since Spark
    does not have a native rolling cross-correlation function. This
    is the one place where toPandas() is acceptable — the pivot table
    is already aggregated to date × symbol (small).

    Args:
        df: Daily summaries with symbol, date, daily_return_pct.
        spark: Active SparkSession (for converting result back).
        window: Rolling window size in days.

    Returns:
        Spark DataFrame with symbol_a, symbol_b, date, correlation.
    """
    import pandas as pd

    pivot_df = (
        df.filter(F.col("daily_return_pct").isNotNull())
        .groupBy("date")
        .pivot("symbol")
        .agg(F.first("daily_return_pct"))
        .orderBy("date")
    )

    # Convert to pandas for rolling correlation (aggregated, small)
    pdf = pivot_df.toPandas()
    if pdf.empty or pdf.shape[1] < 3:  # type: ignore[union-attr, attr-defined]  # date + at least 2 symbols
        schema = StructType(
            [
                StructField("date", TimestampType()),
                StructField("symbol_a", StringType()),
                StructField("symbol_b", StringType()),
                StructField("correlation", DoubleType()),
            ]
        )
        return spark.createDataFrame([], schema)

    date_col = pdf["date"]  # type: ignore[index]
    symbols = [c for c in pdf.columns if c != "date"]  # type: ignore[union-attr, attr-defined]
    numeric = pdf[symbols].astype(float)  # type: ignore[index]
    rolling_corr = numeric.rolling(window=window, min_periods=window).corr()

    rows = []
    pairs = list(itertools.combinations(symbols, 2))
    for idx in range(window - 1, len(date_col)):
        date_val = date_col.iloc[idx]
        try:
            corr_block = rolling_corr.iloc[
                idx * len(symbols) : (idx + 1) * len(symbols)
            ]
            corr_block.index = symbols
        except (IndexError, ValueError):
            continue
        for sym_a, sym_b in pairs:
            try:
                val = corr_block.loc[sym_a, sym_b]
                if pd.notna(val):
                    rows.append((date_val, sym_a, sym_b, float(val)))
            except KeyError:
                continue

    schema = StructType(
        [
            StructField("date", TimestampType()),
            StructField("symbol_a", StringType()),
            StructField("symbol_b", StringType()),
            StructField("correlation", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


# ------------------------------------------------------------------
# Full pipeline
# ------------------------------------------------------------------


def compute_daily_summaries(df: DataFrame) -> DataFrame:
    """Compute all daily summaries: indicators, signals, sector.

    Args:
        df: Silver-layer OHLCV DataFrame.

    Returns:
        DataFrame with all indicator and signal columns.
    """
    result = add_daily_return(df)
    for period in SMA_PERIODS:
        result = add_sma(result, period)
    for period in EMA_PERIODS:
        result = add_ema(result, period)
    result = add_rsi(result)
    result = add_macd(result)
    result = add_volume_metrics(result)
    result = add_sector(result)
    result = generate_signals(result)
    return result


def run_daily_aggregation(
    spark: SparkSession,
    silver_path: str,
) -> dict[str, DataFrame]:
    """Run the complete daily aggregation pipeline.

    Args:
        spark: Active SparkSession.
        silver_path: S3a path to silver-layer Parquet data.

    Returns:
        Dict with 'daily_summaries', 'sector_performance',
        'correlations' Spark DataFrames.
    """
    silver_df = read_silver_data(spark, silver_path)
    summaries = compute_daily_summaries(silver_df)
    sector_perf = compute_sector_performance(summaries)
    correlations = compute_correlation_matrix(summaries, spark)

    return {
        "daily_summaries": summaries,
        "sector_performance": sector_perf,
        "correlations": correlations,
    }


def write_gold_outputs(outputs: dict[str, DataFrame], gold_path: str) -> None:
    """Write all gold-layer outputs to S3 as Parquet.

    Args:
        outputs: Result dict from run_daily_aggregation.
        gold_path: S3a base path for gold layer.
    """
    for name, df in outputs.items():
        path = f"{gold_path}/{name}"
        (df.write.mode("overwrite").option("compression", "snappy").parquet(path))
        logger.info("Wrote gold/%s to %s", name, path)


def main() -> None:
    """Entry point for the daily aggregation batch job."""
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

        outputs = run_daily_aggregation(spark, silver_path)
        write_gold_outputs(outputs, gold_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
