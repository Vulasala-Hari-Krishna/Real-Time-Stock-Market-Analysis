"""Ticks-to-historical rollup transform.

Mirrors the legacy ``tick_rollup.py``'s exact role - "roll up already-
captured data into daily OHLCV bars, append to the historical table" -
without ever calling an external API. Legacy rolls up raw ticks from
``silver/stock_ticks``; this rolls up ``landed_ticks``' already-published
gold ``daily_quote_summary`` (itself an aggregate of sampled quotes this
project's own Kafka -> Databricks path already captured). Same underlying
principle as ``tick_rollup.py``: the expensive external fetch only ever
happens once (or rarely, on a deliberate re-backfill); the daily sync reuses
what the stream already captured, at zero extra API cost.

See ``src/batch/databricks_ticks_rollup.py`` for the job runner that appends
this transform's output to ``historical_ohlcv``/``historical_ohlcv_raw`` -
the same tables ``databricks_historical.py``'s one-time/manual yfinance
backfill owns, mirroring how legacy's ``initial_historical_backfill.py`` and
``tick_rollup.py`` both write to the same ``silver/historical`` target.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_LABEL = "ticks_rollup"


def project_quote_summary_as_ohlcv(quote_summary: DataFrame) -> DataFrame:
    """Map ``daily_quote_summary``'s sampled-quote columns to an OHLCV shape.

    The same mapping ``tick_rollup.py`` applies to raw ticks - first
    observed price = open, max = high, min = low, last observed price =
    close, last reported = volume - just against an already-aggregated
    source instead of raw ticks directly.

    Args:
        quote_summary: ``daily_quote_summary`` rows (``provider``,
            ``symbol``, ``capture_date_utc``, ``first_observed_price``,
            ``highest_observed_price``, ``lowest_observed_price``,
            ``last_observed_price``, ``last_reported_volume``, ...).

    Returns:
        Rows shaped like ``historical_ohlcv``'s bronze schema (``symbol``,
        ``date``, ``open``, ``high``, ``low``, ``close``, ``volume``,
        ``source``) - missing only ``extraction_id``/``bronze_ingested_at``,
        added by the caller at append time. ``date`` is cast from
        ``capture_date_utc`` (already a clean calendar ``DATE``, per
        ``landed_ticks.py::summarize_quotes``) to ``timestamp`` at midnight -
        matching ``historical_ohlcv``'s own ``date timestamp`` column type
        and its "clean calendar day, no time-of-day" convention (see
        ``landed_historical.py``'s ``fetch_all_history`` for why that
        convention matters - a real Snowflake DATE-cast bug otherwise).
    """
    return quote_summary.select(
        F.col("symbol"),
        F.col("capture_date_utc").cast("timestamp").alias("date"),
        F.col("first_observed_price").cast("double").alias("open"),
        F.col("highest_observed_price").cast("double").alias("high"),
        F.col("lowest_observed_price").cast("double").alias("low"),
        F.col("last_observed_price").cast("double").alias("close"),
        F.col("last_reported_volume").cast("long").alias("volume"),
        F.lit(SOURCE_LABEL).alias("source"),
    )
