"""Shared test fixtures for the stock market pipeline tests."""

from datetime import datetime, timezone

import pytest


@pytest.fixture()
def sample_prices() -> list[float]:
    """A realistic 30-day closing price series for testing indicators."""
    return [
        150.0,
        152.0,
        149.0,
        153.0,
        155.0,
        154.0,
        156.0,
        158.0,
        157.0,
        160.0,
        162.0,
        161.0,
        163.0,
        165.0,
        164.0,
        166.0,
        168.0,
        167.0,
        170.0,
        172.0,
        171.0,
        173.0,
        175.0,
        174.0,
        176.0,
        178.0,
        177.0,
        180.0,
        182.0,
        181.0,
    ]


@pytest.fixture()
def sample_volumes() -> list[int]:
    """A realistic 25-day volume series for testing volume anomaly detection."""
    return [
        1000000,
        1100000,
        950000,
        1050000,
        1200000,
        1000000,
        1150000,
        980000,
        1080000,
        1100000,
        1020000,
        1060000,
        1000000,
        1130000,
        1050000,
        1070000,
        1000000,
        1090000,
        1040000,
        1110000,
        1000000,
        1050000,
        1080000,
        1020000,
        5000000,
    ]


@pytest.fixture()
def sample_tick_data() -> dict:
    """Valid data dictionary for constructing a StockTick model."""
    return {
        "symbol": "AAPL",
        "price": 178.50,
        "volume": 52_000_000,
        "timestamp": datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc),
        "source": "alpha_vantage",
    }


@pytest.fixture()
def sample_ohlcv_data() -> dict:
    """Valid data dictionary for constructing an OHLCVRecord model."""
    return {
        "symbol": "AAPL",
        "date": datetime(2024, 3, 15, tzinfo=timezone.utc),
        "open": 175.00,
        "high": 180.00,
        "low": 174.50,
        "close": 178.50,
        "volume": 52_000_000,
        "source": "yfinance",
    }


@pytest.fixture()
def sample_fundamental_data() -> dict:
    """Valid data dictionary for constructing a FundamentalData model."""
    return {
        "symbol": "AAPL",
        "retrieved_at": datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
        "market_cap": 2_800_000_000_000.0,
        "pe_ratio": 28.5,
        "forward_pe": 26.0,
        "dividend_yield": 0.005,
        "eps": 6.25,
        "beta": 1.2,
        "fifty_two_week_high": 199.62,
        "fifty_two_week_low": 143.90,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }


@pytest.fixture()
def sample_daily_summary_data() -> dict:
    """Valid data dictionary for constructing a DailySummary model."""
    return {
        "symbol": "AAPL",
        "date": datetime(2024, 3, 15, tzinfo=timezone.utc),
        "open": 175.00,
        "high": 180.00,
        "low": 174.50,
        "close": 178.50,
        "volume": 52_000_000,
        "daily_return_pct": 1.25,
        "sma_20": 172.30,
        "sma_50": 168.50,
        "rsi_14": 62.5,
        "volume_anomaly": False,
        "sector": "Technology",
    }
