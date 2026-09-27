"""Snowflake-backed loaders for every hybrid dashboard dataset (R5).

Connects read-only, as the least-privilege ``reader`` role created by
``snowflake/terraform/workspace/main.tf`` - never the R4 loader role, never
ACCOUNTADMIN. Unlike a silent demo-data fallback, this never lets a failed
Snowflake connection masquerade as a real result: every call returns an
explicit ``LoadStatus`` alongside the DataFrame, and the page must render
it as a visible banner, not a quiet log line.

One dataset registry (``_REGISTRY``) drives every dataset the hybrid
pipeline publishes to ``SERVING.*`` - ``daily_quote_summary`` (R5's
original dataset), plus ``daily_summaries``/``sector_performance``/
``correlations``/``fundamentals`` (already loaded by
``databricks_indicators.py``/``databricks_fundamentals.py`` via
``src/load/snowflake_snapshot.py``'s ``DATASET_SERVING_TABLE`` - the same
table names reused here, not reinvented). Adding a dataset means adding one
registry entry, not a second copy of the fetch/cache/demo-fallback
machinery.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import numpy as np
import pandas as pd
import streamlit as st

from dashboards import local_cache
from dashboards.data_loader import SECTOR_MAP, SYMBOLS, WATCHLIST
from dashboards.load_status import LoadStatus

logger = logging.getLogger(__name__)


def _private_key_der(pem_text: str, passphrase: Optional[str]) -> bytes:
    from cryptography.hazmat.primitives import serialization

    # A local .env file (unlike a GitHub Actions secret) cannot reliably
    # hold a real multi-line value across every Docker Compose version, so
    # the documented local setup stores this as one line with literal
    # "\n" escapes. Un-escaping is always safe even when the value already
    # has real newlines (a valid PEM body never contains a literal
    # backslash - it's base64), so this handles both formats.
    pem_text = pem_text.replace("\\n", "\n")
    password = passphrase.encode("utf-8") if passphrase else None
    key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"), password=password
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    """Open a key-pair authenticated Snowflake connection as the read-only
    reader role. Raises if any required env var/credential is missing or
    the connection fails - callers must not treat that as demo-worthy,
    only as a clearly-flagged failure."""
    import snowflake.connector as sf

    pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    return sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_READER_ROLE", "STOCK_MARKET_DEV_READER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "STOCK_MARKET_DEV_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "STOCK_MARKET_DEV"),
        schema=os.environ.get("SNOWFLAKE_SERVING_SCHEMA", "SERVING"),
        private_key=_private_key_der(pem, passphrase),
    )


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_from_snowflake(dataset: str, serving_table: str) -> pd.DataFrame:
    """Query ``SERVING.<serving_table>``. Cached for 5 minutes per dataset
    so repeated page renders/reruns don't re-open a warehouse session each
    time. Streamlit's cache does not store a raised exception, so a
    transient failure is retried on the next call rather than getting
    stuck."""
    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {serving_table}")
        columns = [col[0].lower() for col in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        conn.close()


def _generate_demo_daily_quote_summary() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.DAILY_QUOTE_SUMMARY.

    Only ever paired with an explicit LoadStatus(source="demo", ok=False) -
    never returned silently as if it were a real Snowflake result.
    """
    rng = np.random.default_rng(7)
    n_days = 60
    dates = pd.bdate_range(end=datetime.now(timezone.utc).date(), periods=n_days)
    base_prices = {
        "AAPL": 175.0,
        "MSFT": 420.0,
        "GOOGL": 155.0,
        "AMZN": 185.0,
        "TSLA": 250.0,
        "META": 500.0,
        "NVDA": 880.0,
        "JPM": 195.0,
        "V": 280.0,
        "JNJ": 155.0,
    }
    rows = []
    for symbol in SYMBOLS:
        price = base_prices.get(symbol, 100.0)
        for date in dates:
            open_price = price
            price *= 1 + rng.normal(0.0004, 0.012)
            high = max(open_price, price) * (1 + rng.uniform(0, 0.004))
            low = min(open_price, price) * (1 - rng.uniform(0, 0.004))
            rows.append(
                {
                    "provider": "demo",
                    "symbol": symbol,
                    "capture_date_utc": date.date(),
                    "first_observed_price": round(open_price, 2),
                    "highest_observed_price": round(high, 2),
                    "lowest_observed_price": round(low, 2),
                    "last_observed_price": round(price, 2),
                    "last_reported_volume": int(rng.integers(200_000, 3_000_000)),
                    "quote_count": int(rng.integers(20, 200)),
                    "first_quote_at": pd.Timestamp(date, tz="UTC"),
                    "last_quote_at": pd.Timestamp(date, tz="UTC")
                    + pd.Timedelta(hours=8),
                    "observed_change_pct": round((price / open_price - 1) * 100, 4),
                    "bronze_version": None,
                    "loaded_batch_id": "demo",
                    "loaded_at": pd.Timestamp.now(tz="UTC"),
                }
            )
    return pd.DataFrame(rows)


def _generate_demo_summaries() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.DAILY_SUMMARIES."""
    rng = np.random.default_rng(42)
    n_days = 200
    dates = pd.bdate_range(end=datetime.now(timezone.utc).date(), periods=n_days)
    rows = []

    base_prices = {
        "AAPL": 175.0,
        "MSFT": 420.0,
        "GOOGL": 155.0,
        "AMZN": 185.0,
        "TSLA": 250.0,
        "META": 500.0,
        "NVDA": 880.0,
        "JPM": 195.0,
        "V": 280.0,
        "JNJ": 155.0,
    }

    for symbol in SYMBOLS:
        price = base_prices[symbol]
        for i, date in enumerate(dates):
            ret = rng.normal(0.0005, 0.015)
            price *= 1 + ret
            vol = int(rng.integers(500_000, 5_000_000))
            sma20 = price * (1 + rng.normal(0, 0.005)) if i >= 20 else None
            sma50 = price * (1 + rng.normal(0, 0.008)) if i >= 50 else None
            sma200 = price * (1 + rng.normal(0, 0.012)) if i >= 200 else None
            rsi = float(rng.uniform(25, 75))
            vol_vs_avg = float(rng.uniform(0.5, 1.8))

            signals = []
            if rsi > 70:
                signals.append("OVERBOUGHT")
            elif rsi < 30:
                signals.append("OVERSOLD")
            if vol_vs_avg > 2.0:
                signals.append("VOLUME_SPIKE")

            rows.append(
                {
                    "symbol": symbol,
                    "date": date,
                    "open": round(price * 0.998, 2),
                    "high": round(price * 1.005, 2),
                    "low": round(price * 0.994, 2),
                    "close": round(price, 2),
                    "volume": vol,
                    "daily_return_pct": round(ret * 100, 4),
                    "sma_20": round(sma20, 2) if sma20 else None,
                    "sma_50": round(sma50, 2) if sma50 else None,
                    "sma_200": round(sma200, 2) if sma200 else None,
                    "ema_12": round(price * (1 + rng.normal(0, 0.003)), 2),
                    "ema_26": round(price * (1 + rng.normal(0, 0.005)), 2),
                    "rsi_14": round(rsi, 2),
                    "macd_line": round(float(rng.normal(0, 2)), 4),
                    "macd_signal": round(float(rng.normal(0, 1.5)), 4),
                    "macd_histogram": round(float(rng.normal(0, 1)), 4),
                    "volume_vs_avg": round(vol_vs_avg, 4),
                    "sector": SECTOR_MAP[symbol],
                    "signals": ",".join(signals),
                }
            )
    return pd.DataFrame(rows)


def _generate_demo_sector() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.SECTOR_PERFORMANCE."""
    rng = np.random.default_rng(42)
    n_days = 60
    dates = pd.bdate_range(end=datetime.now(timezone.utc).date(), periods=n_days)
    sectors = list({s["sector"] for s in WATCHLIST})
    rows = []
    for date in dates:
        for sector in sectors:
            sector_syms = [s["symbol"] for s in WATCHLIST if s["sector"] == sector]
            rows.append(
                {
                    "sector": sector,
                    "date": date,
                    "avg_return_pct": round(float(rng.normal(0.05, 0.8)), 4),
                    "top_performer": rng.choice(sector_syms),
                    "bottom_performer": rng.choice(sector_syms),
                }
            )
    return pd.DataFrame(rows)


def _generate_demo_correlations() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.CORRELATIONS."""
    import itertools

    rng = np.random.default_rng(42)
    pairs = list(itertools.combinations(SYMBOLS, 2))
    rows = []
    for sym_a, sym_b in pairs:
        rows.append(
            {
                "date": datetime.now(timezone.utc).date(),
                "symbol_a": sym_a,
                "symbol_b": sym_b,
                "correlation": round(float(rng.uniform(-0.3, 0.95)), 4),
            }
        )
    return pd.DataFrame(rows)


def _generate_demo_fundamentals() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.FUNDAMENTALS."""
    data = {
        "AAPL": {
            "market_cap": 2.8e12,
            "pe_ratio": 28.5,
            "forward_pe": 26.0,
            "dividend_yield": 0.005,
            "eps": 6.25,
            "beta": 1.2,
            "fifty_two_week_high": 199.6,
            "fifty_two_week_low": 143.9,
        },
        "MSFT": {
            "market_cap": 3.1e12,
            "pe_ratio": 35.2,
            "forward_pe": 30.0,
            "dividend_yield": 0.007,
            "eps": 11.8,
            "beta": 0.9,
            "fifty_two_week_high": 450.0,
            "fifty_two_week_low": 310.0,
        },
        "GOOGL": {
            "market_cap": 1.9e12,
            "pe_ratio": 25.0,
            "forward_pe": 22.0,
            "dividend_yield": 0.0,
            "eps": 6.5,
            "beta": 1.1,
            "fifty_two_week_high": 175.0,
            "fifty_two_week_low": 120.0,
        },
        "AMZN": {
            "market_cap": 1.9e12,
            "pe_ratio": 60.0,
            "forward_pe": 45.0,
            "dividend_yield": 0.0,
            "eps": 3.0,
            "beta": 1.3,
            "fifty_two_week_high": 200.0,
            "fifty_two_week_low": 140.0,
        },
        "TSLA": {
            "market_cap": 0.8e12,
            "pe_ratio": 75.0,
            "forward_pe": 55.0,
            "dividend_yield": 0.0,
            "eps": 3.4,
            "beta": 2.0,
            "fifty_two_week_high": 300.0,
            "fifty_two_week_low": 150.0,
        },
        "META": {
            "market_cap": 1.3e12,
            "pe_ratio": 27.0,
            "forward_pe": 22.0,
            "dividend_yield": 0.004,
            "eps": 18.0,
            "beta": 1.2,
            "fifty_two_week_high": 540.0,
            "fifty_two_week_low": 370.0,
        },
        "NVDA": {
            "market_cap": 2.2e12,
            "pe_ratio": 65.0,
            "forward_pe": 40.0,
            "dividend_yield": 0.0004,
            "eps": 13.5,
            "beta": 1.7,
            "fifty_two_week_high": 950.0,
            "fifty_two_week_low": 470.0,
        },
        "JPM": {
            "market_cap": 0.55e12,
            "pe_ratio": 12.0,
            "forward_pe": 11.0,
            "dividend_yield": 0.023,
            "eps": 16.0,
            "beta": 1.1,
            "fifty_two_week_high": 210.0,
            "fifty_two_week_low": 155.0,
        },
        "V": {
            "market_cap": 0.58e12,
            "pe_ratio": 30.0,
            "forward_pe": 25.0,
            "dividend_yield": 0.008,
            "eps": 9.3,
            "beta": 0.95,
            "fifty_two_week_high": 295.0,
            "fifty_two_week_low": 240.0,
        },
        "JNJ": {
            "market_cap": 0.37e12,
            "pe_ratio": 22.0,
            "forward_pe": 15.0,
            "dividend_yield": 0.03,
            "eps": 7.0,
            "beta": 0.55,
            "fifty_two_week_high": 175.0,
            "fifty_two_week_low": 140.0,
        },
    }
    rows = []
    for sym, vals in data.items():
        row = {"symbol": sym, **vals, "yf_sector": SECTOR_MAP[sym], "industry": "N/A"}
        rows.append(row)
    return pd.DataFrame(rows)


@dataclass
class _DatasetSpec:
    serving_table: str
    demo_generator: Callable[[], pd.DataFrame]


_REGISTRY: dict[str, _DatasetSpec] = {
    "daily_quote_summary": _DatasetSpec(
        "DAILY_QUOTE_SUMMARY", _generate_demo_daily_quote_summary
    ),
    "daily_summaries": _DatasetSpec("DAILY_SUMMARIES", _generate_demo_summaries),
    "sector_performance": _DatasetSpec("SECTOR_PERFORMANCE", _generate_demo_sector),
    "correlations": _DatasetSpec("CORRELATIONS", _generate_demo_correlations),
    "fundamentals": _DatasetSpec("FUNDAMENTALS", _generate_demo_fundamentals),
}


def load_dataset(dataset: str) -> tuple[pd.DataFrame, LoadStatus]:
    """Load one hybrid dataset from Snowflake, with an honest fallback.

    Returns:
        The data, and an explicit LoadStatus the caller must render as a
        visible banner. On failure this prefers the last known-good local
        cache (source="cache", ok=False - real data, just stale) over
        synthetic demo data; demo data (source="demo", ok=False) is only
        used when no cache exists yet, and is never presented as real.
    """
    spec = _REGISTRY[dataset]
    try:
        df = _fetch_from_snowflake(dataset, spec.serving_table)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        as_of = (
            pd.to_datetime(df["loaded_at"]).max()
            if "loaded_at" in df.columns and not df.empty
            else None
        )
        batch_id = (
            df["loaded_batch_id"].iloc[0]
            if "loaded_batch_id" in df.columns and not df.empty
            else None
        )
        local_cache.save_snapshot(dataset, df, {"as_of": as_of, "batch_id": batch_id})
        return df, LoadStatus(
            source="snowflake",
            ok=True,
            message=f"Loaded {len(df)} rows from Snowflake",
            as_of=as_of,
            batch_id=batch_id,
        )
    except Exception as exc:
        logger.exception("Snowflake load failed for %s", dataset)
        cached = local_cache.load_snapshot(dataset)
        if cached is not None:
            cached_df, metadata = cached
            as_of = metadata.get("as_of")
            return cached_df, LoadStatus(
                source="cache",
                ok=False,
                message=(
                    f"Snowflake unavailable ({exc}); showing cached data "
                    f"from {as_of}"
                ),
                as_of=pd.to_datetime(as_of) if as_of else None,
                batch_id=metadata.get("batch_id"),
            )
        return spec.demo_generator(), LoadStatus(
            source="demo",
            ok=False,
            message=f"Snowflake unavailable: {exc}",
        )


def load_daily_quote_summary() -> tuple[pd.DataFrame, LoadStatus]:
    """Load SERVING.DAILY_QUOTE_SUMMARY."""
    return load_dataset("daily_quote_summary")


def load_daily_summaries() -> tuple[pd.DataFrame, LoadStatus]:
    """Load SERVING.DAILY_SUMMARIES."""
    return load_dataset("daily_summaries")


def load_sector_performance() -> tuple[pd.DataFrame, LoadStatus]:
    """Load SERVING.SECTOR_PERFORMANCE."""
    return load_dataset("sector_performance")


def load_correlations() -> tuple[pd.DataFrame, LoadStatus]:
    """Load SERVING.CORRELATIONS."""
    return load_dataset("correlations")


def load_fundamentals() -> tuple[pd.DataFrame, LoadStatus]:
    """Load SERVING.FUNDAMENTALS."""
    return load_dataset("fundamentals")
