"""Reusable fetch/validation and Spark transformations for the fundamentals
slice (R6, first migrated legacy product).

Unlike ticks (a continuously-polling local Kafka producer feeding a local
consumer that lands to S3 for Databricks to ingest), fundamentals change
slowly and have no local streaming component to preserve. The yfinance
fetch is business job logic, not infrastructure, so it runs as part of the
Databricks job itself (``databricks_fundamentals.py``) - the same way the
legacy ``fundamental_enrichment.py`` Spark job already fetched yfinance
from inside the job, not from a separate producer. Running this fetch on
a GitHub Actions runner was tried and abandoned: Yahoo Finance persistently
rate-limited every request from GitHub's shared runner IP range even with
retries/backoff (confirmed live 2026-09-26 - all 40 attempts across 10
symbols got 429 Too Many Requests), which is also the wrong place for
business logic to run per this project's ownership boundaries (GitHub
Actions owns infra deploy/update/teardown, not job execution).
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from pydantic import ValidationError
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.schemas import FundamentalData

logger = logging.getLogger(__name__)

# Retry/backoff for a single symbol's transient failure (network blip,
# momentary rate limit) - not a fix for a persistent block, just for the
# ordinary flakiness of an unofficial API.
RETRY_DELAYS_SECONDS: tuple[float, ...] = (2.0, 5.0, 10.0)
SYMBOL_DELAY_SECONDS = 1.5

# yfinance Ticker.info key -> FundamentalData field name.
YFINANCE_FIELD_MAP: dict[str, str] = {
    "marketCap": "market_cap",
    "trailingPE": "pe_ratio",
    "forwardPE": "forward_pe",
    "dividendYield": "dividend_yield",
    "trailingEps": "eps",
    "beta": "beta",
    "fiftyTwoWeekHigh": "fifty_two_week_high",
    "fiftyTwoWeekLow": "fifty_two_week_low",
    "sector": "sector",
    "industry": "industry",
}

BUSINESS_KEYS = ["symbol"]
BRONZE_SCHEMA = (
    "extraction_id string, symbol string, retrieved_at timestamp, "
    "market_cap double, pe_ratio double, forward_pe double, "
    "dividend_yield double, eps double, beta double, "
    "fifty_two_week_high double, fifty_two_week_low double, "
    "sector string, industry string, bronze_ingested_at timestamp"
)


def fetch_one(
    symbol: str, sleep: Callable[[float], None] = time.sleep
) -> FundamentalData | None:
    """Fetch and validate one symbol's fundamentals; never raises.

    Retries transient yfinance/HTTP failures (e.g. a momentary 429) with
    backoff before giving up on this symbol - not a fix for a persistent
    block, just for ordinary transient flakiness.

    Args:
        symbol: Ticker symbol to fetch from yfinance.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        A validated record, or None if every attempt failed/validation
        failed - logged, not raised, so one bad symbol never blocks the
        rest of the run.
    """
    import yfinance as yf

    attempt_delays = (0.0, *RETRY_DELAYS_SECONDS)
    info: dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt, delay in enumerate(attempt_delays):
        if delay:
            sleep(delay)
        try:
            info = yf.Ticker(symbol).info
            last_error = None
            break
        except Exception as exc:  # yfinance raises a mix of exception types
            last_error = exc
            logger.warning(
                "yfinance fetch failed for %s (attempt %d/%d): %s",
                symbol,
                attempt + 1,
                len(attempt_delays),
                exc,
            )
    if last_error is not None or info is None:
        logger.error(
            "yfinance fetch permanently failed for %s after %d attempts",
            symbol,
            len(attempt_delays),
        )
        return None
    fields: dict[str, Any] = {
        target: info.get(source) for source, target in YFINANCE_FIELD_MAP.items()
    }
    try:
        return FundamentalData(
            symbol=symbol,
            retrieved_at=datetime.now(timezone.utc),
            **fields,
        )
    except ValidationError:
        logger.exception("Fundamentals validation failed for %s", symbol)
        return None


def fetch_all(
    symbols: list[str], sleep: Callable[[float], None] = time.sleep
) -> list[dict[str, Any]]:
    """Fetch every symbol, spacing requests apart, into bronze-ready rows.

    Args:
        symbols: Ticker symbols to fetch.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        One dict per successfully fetched/validated symbol, each carrying
        this run's shared ``extraction_id`` for bronze lineage. Failed
        symbols are simply absent - logged by ``fetch_one``, not modeled
        as a business record, since the failure happened in this same
        trusted process (nothing untrusted was ever persisted to reject).
    """
    extraction_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    rows = []
    for index, symbol in enumerate(symbols):
        if index:
            # Space requests apart to reduce the chance of tripping a rate
            # limit in the first place, not just retrying after the fact.
            sleep(SYMBOL_DELAY_SECONDS)
        record = fetch_one(symbol, sleep=sleep)
        if record is not None:
            row = record.model_dump(mode="json")
            row["extraction_id"] = extraction_id
            rows.append(row)
    return rows


def rank_latest_per_symbol(bronze: DataFrame) -> DataFrame:
    """Keep only the most recent bronze row per symbol, across all runs.

    Bronze is append-only across job runs (every run is a genuine new
    observation, not a replay of previously-seen data - there is no file
    checkpoint to make idempotent). Gold always reflects the latest
    observation per symbol, so a symbol dropped from being fetchable
    still shows its last known values until a newer fetch supersedes them.

    Args:
        bronze: The full accumulated bronze history for this dataset.

    Returns:
        One row per symbol - its most recent observation.
    """
    order = Window.partitionBy(*BUSINESS_KEYS).orderBy(
        F.col("retrieved_at").desc(),
        F.col("bronze_ingested_at").desc(),
        "extraction_id",
    )
    return (
        bronze.withColumn("rank", F.row_number().over(order))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def project_fundamentals(latest: DataFrame) -> DataFrame:
    """Drop bronze-only bookkeeping columns, leaving the gold-ready projection.

    Args:
        latest: The most recent row per symbol (see rank_latest_per_symbol).

    Returns:
        One row per symbol with only the published fundamental fields.
    """
    return latest.select(
        "symbol",
        "retrieved_at",
        "market_cap",
        "pe_ratio",
        "forward_pe",
        "dividend_yield",
        "eps",
        "beta",
        "fifty_two_week_high",
        "fifty_two_week_low",
        "sector",
        "industry",
    )
