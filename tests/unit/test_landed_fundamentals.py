"""Unit tests for src/batch/landed_fundamentals.py (R6, direct-fetch design)."""

from unittest.mock import MagicMock, patch

from src.batch import landed_fundamentals as lf


def _fake_ticker_info(**overrides: object) -> dict:
    base = {
        "marketCap": 2.8e12,
        "trailingPE": 28.5,
        "forwardPE": 26.0,
        "dividendYield": 0.005,
        "trailingEps": 6.25,
        "beta": 1.2,
        "fiftyTwoWeekHigh": 199.6,
        "fiftyTwoWeekLow": 143.9,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------
def test_fetch_one_returns_a_validated_record() -> None:
    with patch("yfinance.Ticker") as mock_ticker:
        mock_ticker.return_value.info = _fake_ticker_info()
        record = lf.fetch_one("AAPL")

    assert record is not None
    assert record.symbol == "AAPL"
    assert record.market_cap == 2.8e12
    assert record.sector == "Technology"
    assert record.retrieved_at.utcoffset() is not None


def test_fetch_one_returns_none_when_yfinance_always_raises() -> None:
    sleeps: list[float] = []
    with patch("yfinance.Ticker", side_effect=RuntimeError("429 Too Many Requests")):
        assert lf.fetch_one("AAPL", sleep=sleeps.append) is None
    # Retries with backoff before giving up - never blocks a real test run.
    assert sleeps == list(lf.RETRY_DELAYS_SECONDS)


def test_fetch_one_retries_and_recovers_from_a_transient_failure() -> None:
    attempts = {"count": 0}

    def flaky_ticker(symbol: str) -> MagicMock:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("429 Too Many Requests")
        mock = MagicMock()
        mock.info = _fake_ticker_info()
        return mock

    sleeps: list[float] = []
    with patch("yfinance.Ticker", side_effect=flaky_ticker):
        record = lf.fetch_one("AAPL", sleep=sleeps.append)

    assert record is not None
    assert attempts["count"] == 3
    assert sleeps == list(lf.RETRY_DELAYS_SECONDS[:2])


def test_fetch_one_returns_none_on_invalid_fields() -> None:
    with patch("yfinance.Ticker") as mock_ticker:
        mock_ticker.return_value.info = _fake_ticker_info(fiftyTwoWeekHigh=-1)
        assert lf.fetch_one("AAPL") is None


# ---------------------------------------------------------------------------
# fetch_all
# ---------------------------------------------------------------------------
def test_fetch_all_skips_failures_and_tags_every_row_with_one_extraction_id() -> None:
    def fake_ticker(symbol: str) -> MagicMock:
        mock = MagicMock()
        if symbol == "BAD":
            raise RuntimeError("boom")
        mock.info = _fake_ticker_info()
        return mock

    with patch("yfinance.Ticker", side_effect=fake_ticker):
        rows = lf.fetch_all(["AAPL", "BAD", "MSFT"], sleep=lambda _: None)

    assert {row["symbol"] for row in rows} == {"AAPL", "MSFT"}
    assert len({row["extraction_id"] for row in rows}) == 1


def test_fetch_all_returns_empty_list_when_everything_fails() -> None:
    with patch("yfinance.Ticker", side_effect=RuntimeError("boom")):
        rows = lf.fetch_all(["AAPL"], sleep=lambda _: None)
    assert rows == []


# ---------------------------------------------------------------------------
# Spark expression builders (cloud-free boundary check; real DataFrame
# semantics are verified separately, same convention as test_databricks_ticks.py)
# ---------------------------------------------------------------------------
def test_spark_expression_builders_require_no_platform_clients() -> None:
    frame = MagicMock()
    with patch.object(lf, "F") as functions, patch.object(lf, "Window"):
        lf.rank_latest_per_symbol(frame)
        functions.col.assert_any_call("retrieved_at")
        lf.project_fundamentals(frame)
        frame.select.assert_called_with(
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
