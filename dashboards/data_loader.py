"""Data loading utilities for the Streamlit dashboard.

Reads gold-layer Parquet files from S3 via boto3/pandas. Falls back to
demo data when S3 is unavailable (local development).  Also provides
a loader for real-time tick data from the silver layer.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Watchlist (duplicated here to avoid importing the full src tree in
# the lightweight dashboard container — keeps the image small).
WATCHLIST = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Technology"},
    {"symbol": "GOOGL", "name": "Alphabet Inc.", "sector": "Technology"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "META", "name": "Meta Platforms Inc.", "sector": "Technology"},
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology"},
    {"symbol": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Financial Services"},
    {"symbol": "V", "name": "Visa Inc.", "sector": "Financial Services"},
    {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare"},
]
SYMBOLS = [s["symbol"] for s in WATCHLIST]
SECTOR_MAP = {s["symbol"]: s["sector"] for s in WATCHLIST}
NAME_MAP = {s["symbol"]: s["name"] for s in WATCHLIST}


def _s3_path(table: str) -> str:
    """Build an S3 URI for a gold-layer table.

    Args:
        table: Table name (e.g. 'daily_summaries').

    Returns:
        S3 URI string.
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "stock-market-datalake")
    return f"s3://{bucket}/gold/{table}"


def _try_read_s3(table: str) -> Optional[pd.DataFrame]:
    """Attempt to read a Parquet table from S3.

    Args:
        table: Gold-layer table name.

    Returns:
        DataFrame if successful, None otherwise.
    """
    try:
        path = _s3_path(table)
        df = pd.read_parquet(
            path,
            storage_options={
                "key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "client_kwargs": {
                    "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                },
            },
        )
        logger.info("Loaded %d rows from %s", len(df), path)
        return df
    except Exception:
        logger.debug("S3 read failed for %s, using demo data", table)
        return None


def _generate_demo_summaries() -> pd.DataFrame:
    """Generate deterministic demo daily-summary data.

    Returns:
        DataFrame mimicking the gold daily_summaries table.
    """
    rng = np.random.default_rng(42)
    n_days = 200
    dates = pd.bdate_range(end=datetime.utcnow().date(), periods=n_days)
    rows = []

    base_prices = {
        "AAPL": 175.0, "MSFT": 420.0, "GOOGL": 155.0, "AMZN": 185.0,
        "TSLA": 250.0, "META": 500.0, "NVDA": 880.0, "JPM": 195.0,
        "V": 280.0, "JNJ": 155.0,
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

            rows.append({
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
            })
    return pd.DataFrame(rows)


def _generate_demo_sector() -> pd.DataFrame:
    """Generate demo sector performance data.

    Returns:
        DataFrame mimicking gold sector_performance table.
    """
    rng = np.random.default_rng(42)
    n_days = 60
    dates = pd.bdate_range(end=datetime.utcnow().date(), periods=n_days)
    sectors = list({s["sector"] for s in WATCHLIST})
    rows = []
    for date in dates:
        for sector in sectors:
            sector_syms = [s["symbol"] for s in WATCHLIST if s["sector"] == sector]
            rows.append({
                "sector": sector,
                "date": date,
                "avg_return_pct": round(float(rng.normal(0.05, 0.8)), 4),
                "top_performer": rng.choice(sector_syms),
                "bottom_performer": rng.choice(sector_syms),
            })
    return pd.DataFrame(rows)


def _generate_demo_correlations() -> pd.DataFrame:
    """Generate demo pairwise correlation data.

    Returns:
        DataFrame mimicking gold correlations table.
    """
    import itertools
    rng = np.random.default_rng(42)
    pairs = list(itertools.combinations(SYMBOLS, 2))
    rows = []
    for sym_a, sym_b in pairs:
        rows.append({
            "date": datetime.utcnow().date(),
            "symbol_a": sym_a,
            "symbol_b": sym_b,
            "correlation": round(float(rng.uniform(-0.3, 0.95)), 4),
        })
    return pd.DataFrame(rows)


def _generate_demo_fundamentals() -> pd.DataFrame:
    """Generate demo fundamentals data.

    Returns:
        DataFrame mimicking gold fundamentals table.
    """
    data = {
        "AAPL": {"market_cap": 2.8e12, "pe_ratio": 28.5, "forward_pe": 26.0, "dividend_yield": 0.005, "eps": 6.25, "beta": 1.2, "fifty_two_week_high": 199.6, "fifty_two_week_low": 143.9},
        "MSFT": {"market_cap": 3.1e12, "pe_ratio": 35.2, "forward_pe": 30.0, "dividend_yield": 0.007, "eps": 11.8, "beta": 0.9, "fifty_two_week_high": 450.0, "fifty_two_week_low": 310.0},
        "GOOGL": {"market_cap": 1.9e12, "pe_ratio": 25.0, "forward_pe": 22.0, "dividend_yield": 0.0, "eps": 6.5, "beta": 1.1, "fifty_two_week_high": 175.0, "fifty_two_week_low": 120.0},
        "AMZN": {"market_cap": 1.9e12, "pe_ratio": 60.0, "forward_pe": 45.0, "dividend_yield": 0.0, "eps": 3.0, "beta": 1.3, "fifty_two_week_high": 200.0, "fifty_two_week_low": 140.0},
        "TSLA": {"market_cap": 0.8e12, "pe_ratio": 75.0, "forward_pe": 55.0, "dividend_yield": 0.0, "eps": 3.4, "beta": 2.0, "fifty_two_week_high": 300.0, "fifty_two_week_low": 150.0},
        "META": {"market_cap": 1.3e12, "pe_ratio": 27.0, "forward_pe": 22.0, "dividend_yield": 0.004, "eps": 18.0, "beta": 1.2, "fifty_two_week_high": 540.0, "fifty_two_week_low": 370.0},
        "NVDA": {"market_cap": 2.2e12, "pe_ratio": 65.0, "forward_pe": 40.0, "dividend_yield": 0.0004, "eps": 13.5, "beta": 1.7, "fifty_two_week_high": 950.0, "fifty_two_week_low": 470.0},
        "JPM": {"market_cap": 0.55e12, "pe_ratio": 12.0, "forward_pe": 11.0, "dividend_yield": 0.023, "eps": 16.0, "beta": 1.1, "fifty_two_week_high": 210.0, "fifty_two_week_low": 155.0},
        "V": {"market_cap": 0.58e12, "pe_ratio": 30.0, "forward_pe": 25.0, "dividend_yield": 0.008, "eps": 9.3, "beta": 0.95, "fifty_two_week_high": 295.0, "fifty_two_week_low": 240.0},
        "JNJ": {"market_cap": 0.37e12, "pe_ratio": 22.0, "forward_pe": 15.0, "dividend_yield": 0.03, "eps": 7.0, "beta": 0.55, "fifty_two_week_high": 175.0, "fifty_two_week_low": 140.0},
    }
    rows = []
    for sym, vals in data.items():
        row = {"symbol": sym, **vals, "yf_sector": SECTOR_MAP[sym], "industry": "N/A"}
        rows.append(row)
    return pd.DataFrame(rows)


def load_daily_summaries() -> pd.DataFrame:
    """Load gold daily summaries (S3 first, demo fallback).

    Returns:
        Daily summaries DataFrame.
    """
    df = _try_read_s3("daily_summaries")
    if df is None:
        df = _generate_demo_summaries()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def load_sector_performance() -> pd.DataFrame:
    """Load gold sector performance (S3 first, demo fallback).

    Returns:
        Sector performance DataFrame.
    """
    df = _try_read_s3("sector_performance")
    if df is None:
        df = _generate_demo_sector()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def load_correlations() -> pd.DataFrame:
    """Load gold correlations (S3 first, demo fallback).

    Returns:
        Correlations DataFrame.
    """
    df = _try_read_s3("correlations")
    if df is None:
        df = _generate_demo_correlations()
    return df


def load_fundamentals() -> pd.DataFrame:
    """Load gold fundamentals (S3 first, demo fallback).

    Returns:
        Fundamentals DataFrame.
    """
    df = _try_read_s3("fundamentals")
    if df is None:
        df = _generate_demo_fundamentals()
    return df


# ---------------------------------------------------------------------------
# Live tick data (silver layer — speed layer of Lambda Architecture)
# ---------------------------------------------------------------------------


def _s3_silver_path(table: str) -> str:
    """Build an S3 URI for a silver-layer table.

    Args:
        table: Table name (e.g. 'stock_ticks').

    Returns:
        S3 URI string.
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "stock-market-datalake")
    return f"s3://{bucket}/silver/{table}"


def _try_read_silver(table: str) -> Optional[pd.DataFrame]:
    """Attempt to read a Parquet table from the silver layer.

    Args:
        table: Silver-layer table name.

    Returns:
        DataFrame if successful, None otherwise.
    """
    try:
        path = _s3_silver_path(table)
        df = pd.read_parquet(
            path,
            storage_options={
                "key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "client_kwargs": {
                    "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                },
            },
        )
        logger.info("Loaded %d rows from %s", len(df), path)
        return df
    except Exception:
        logger.debug("S3 silver read failed for %s, using demo data", table)
        return None


def _generate_demo_live_ticks() -> pd.DataFrame:
    """Generate deterministic demo live tick data.

    Simulates real-time ticks from the last few hours for all symbols.

    Returns:
        DataFrame mimicking silver/stock_ticks Parquet data.
    """
    rng = np.random.default_rng(99)
    now = datetime.utcnow()
    rows = []

    base_prices = {
        "AAPL": 195.0, "MSFT": 425.0, "GOOGL": 160.0, "AMZN": 190.0,
        "TSLA": 245.0, "META": 510.0, "NVDA": 900.0, "JPM": 200.0,
        "V": 285.0, "JNJ": 158.0,
    }

    for symbol in SYMBOLS:
        price = base_prices[symbol]
        # Simulate ~30 ticks per symbol over the last 60 minutes
        for i in range(30):
            minutes_ago = 60 - (i * 2)
            ts = now - timedelta(minutes=minutes_ago)
            ret = rng.normal(0, 0.002)
            price *= 1 + ret
            vol = int(rng.integers(10_000, 200_000))
            rows.append({
                "symbol": symbol,
                "price": round(price, 2),
                "volume": vol,
                "timestamp": ts,
                "source": "alpha_vantage",
            })

    return pd.DataFrame(rows)


def load_live_ticks() -> pd.DataFrame:
    """Load real-time tick data (S3 silver first, demo fallback).

    Returns:
        Tick-level DataFrame with symbol, price, volume, timestamp.
    """
    df = _try_read_silver("stock_ticks")
    if df is None:
        df = _generate_demo_live_ticks()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df
