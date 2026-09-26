"""Reusable fetch/validation and Spark transformations for historical OHLCV
(R6's second migrated legacy product: historical_backfill.py).

``tick_rollup.py``'s role (rolling up live-captured ticks into daily bars)
is already served by R1's ``daily_quote_summary`` gold table - this job
only ports ``historical_backfill.py``'s distinct capability, a yfinance-based
multi-year backfill that does not depend on any other Databricks table, not
a duplicate of R1's real-time path.

Reuses ``historical_backfill.py::download_history`` directly (the same
dual-path fetch - a direct Yahoo chart API call, falling back to yfinance -
already proven and unit-tested in the legacy job) rather than re-implementing
it, adding only retry/backoff around it (the same transient-failure
mitigation built for fundamentals) and per-symbol pacing.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.batch.historical_backfill import download_history

logger = logging.getLogger(__name__)

BACKFILL_YEARS = 5
RETRY_DELAYS_SECONDS: tuple[float, ...] = (2.0, 5.0, 10.0)
SYMBOL_DELAY_SECONDS = 1.5

BUSINESS_KEYS = ["symbol", "date"]
BRONZE_SCHEMA = (
    "extraction_id string, symbol string, date timestamp, "
    "open double, high double, low double, close double, volume long, "
    "source string"
)


def fetch_one_history(
    symbol: str,
    years: int = BACKFILL_YEARS,
    sleep: Callable[[float], None] = time.sleep,
) -> pd.DataFrame:
    """Download one symbol's OHLCV history with retry/backoff; never raises.

    Args:
        symbol: Ticker symbol to fetch.
        years: Years of history to request.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        A DataFrame from ``download_history``, or empty if every attempt
        failed/returned nothing - logged, not raised, so one bad symbol
        never blocks the rest of the run.
    """
    attempt_delays = (0.0, *RETRY_DELAYS_SECONDS)
    last_error: Exception | str | None = None
    for attempt, delay in enumerate(attempt_delays):
        if delay:
            sleep(delay)
        try:
            df = download_history(symbol, years=years)
        except Exception as exc:  # yfinance/requests raise a mix of types
            last_error = exc
        else:
            if not df.empty:
                return df
            last_error = "empty result"
        logger.warning(
            "History download failed for %s (attempt %d/%d): %s",
            symbol,
            attempt + 1,
            len(attempt_delays),
            last_error,
        )
    logger.error(
        "History download permanently failed for %s after %d attempts",
        symbol,
        len(attempt_delays),
    )
    return pd.DataFrame()


def fetch_all_history(
    symbols: list[str],
    years: int = BACKFILL_YEARS,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Fetch every symbol's history, spacing requests apart.

    Args:
        symbols: Ticker symbols to fetch.
        years: Years of history to request per symbol.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        One dict per (symbol, date) row across every successfully fetched
        symbol, each carrying this run's shared ``extraction_id`` for
        bronze lineage. Failed symbols are simply absent.
    """
    extraction_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    logger.info(
        "Starting history fetch of %d symbols (extraction_id=%s)",
        len(symbols),
        extraction_id,
    )
    rows: list[dict[str, Any]] = []
    for index, symbol in enumerate(symbols):
        if index:
            sleep(SYMBOL_DELAY_SECONDS)
        pdf = fetch_one_history(symbol, years=years, sleep=sleep)
        if not pdf.empty:
            pdf = pdf[
                ["symbol", "date", "open", "high", "low", "close", "volume"]
            ].copy()
            pdf["date"] = pd.to_datetime(pdf["date"], utc=True)
            pdf["open"] = pdf["open"].astype(float)
            pdf["high"] = pdf["high"].astype(float)
            pdf["low"] = pdf["low"].astype(float)
            pdf["close"] = pdf["close"].astype(float)
            pdf["volume"] = pdf["volume"].astype("int64")
            pdf["source"] = "yfinance"
            pdf["extraction_id"] = extraction_id
            rows.extend(pdf.to_dict("records"))
        logger.info(
            "Progress: %d/%d symbols attempted (%s: %s) - %d rows so far",
            index + 1,
            len(symbols),
            symbol,
            "ok" if not pdf.empty else "failed",
            len(rows),
        )
    logger.info(
        "Finished history fetch: %d rows across %d symbols (extraction_id=%s)",
        len(rows),
        len(symbols),
        extraction_id,
    )
    return rows


def rank_latest_per_symbol_date(bronze: DataFrame) -> DataFrame:
    """Keep only the most recently fetched row per (symbol, date).

    Bronze is append-only across runs; a later re-fetch of the same
    historical date can carry a correction (e.g. a retroactive
    split/dividend adjustment), so the latest fetch wins - the same
    latest-wins rule fundamentals uses, just keyed by (symbol, date)
    instead of (symbol) alone since this is a time series.

    Args:
        bronze: The full accumulated bronze history for this dataset.

    Returns:
        One row per (symbol, date) - its most recently fetched observation.
    """
    order = Window.partitionBy(*BUSINESS_KEYS).orderBy(
        F.col("bronze_ingested_at").desc(), "extraction_id"
    )
    return (
        bronze.withColumn("rank", F.row_number().over(order))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def project_historical(latest: DataFrame) -> DataFrame:
    """Drop bronze-only bookkeeping columns, leaving the gold-ready projection.

    Args:
        latest: The most recent row per (symbol, date) (see
            rank_latest_per_symbol_date).

    Returns:
        One row per (symbol, date) with only the published OHLCV fields.
    """
    return latest.select(
        "symbol", "date", "open", "high", "low", "close", "volume", "source"
    )
