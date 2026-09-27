"""Unit tests for src/batch/landed_historical.py (R6, historical OHLCV)."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.batch import landed_historical as lh


def _fake_history(symbol: str, rows: int = 3) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=rows)
    return pd.DataFrame(
        {
            "date": dates,
            "open": [100.0 + i for i in range(rows)],
            "high": [101.0 + i for i in range(rows)],
            "low": [99.0 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [1000 + i for i in range(rows)],
            "symbol": [symbol] * rows,
        }
    )


# ---------------------------------------------------------------------------
# fetch_one_history
# ---------------------------------------------------------------------------
def test_fetch_one_history_returns_the_download_result() -> None:
    with patch.object(lh, "download_history", return_value=_fake_history("AAPL")):
        df = lh.fetch_one_history("AAPL")
    assert not df.empty
    assert len(df) == 3


def test_fetch_one_history_retries_and_recovers_from_a_transient_failure() -> None:
    attempts = {"count": 0}

    def flaky(symbol: str, years: int) -> pd.DataFrame:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("429 Too Many Requests")
        return _fake_history(symbol)

    sleeps: list[float] = []
    with patch.object(lh, "download_history", side_effect=flaky):
        df = lh.fetch_one_history("AAPL", sleep=sleeps.append)

    assert not df.empty
    assert attempts["count"] == 3
    assert sleeps == list(lh.RETRY_DELAYS_SECONDS[:2])


def test_fetch_one_history_returns_empty_when_always_empty() -> None:
    sleeps: list[float] = []
    with patch.object(lh, "download_history", return_value=pd.DataFrame()):
        df = lh.fetch_one_history("AAPL", sleep=sleeps.append)
    assert df.empty
    assert sleeps == list(lh.RETRY_DELAYS_SECONDS)


def test_fetch_one_history_returns_empty_when_always_raises() -> None:
    with patch.object(lh, "download_history", side_effect=RuntimeError("boom")):
        df = lh.fetch_one_history("AAPL", sleep=lambda _: None)
    assert df.empty


# ---------------------------------------------------------------------------
# fetch_all_history
# ---------------------------------------------------------------------------
def test_fetch_all_history_skips_failures_and_tags_one_extraction_id() -> None:
    def fake_download(symbol: str, years: int) -> pd.DataFrame:
        if symbol == "BAD":
            return pd.DataFrame()
        return _fake_history(symbol, rows=2)

    with patch.object(lh, "download_history", side_effect=fake_download):
        rows = lh.fetch_all_history(["AAPL", "BAD", "MSFT"], sleep=lambda _: None)

    symbols_seen = {row["symbol"] for row in rows}
    assert symbols_seen == {"AAPL", "MSFT"}
    assert len(rows) == 4  # 2 rows each for AAPL and MSFT
    assert len({row["extraction_id"] for row in rows}) == 1


def test_fetch_all_history_returns_empty_list_when_everything_fails() -> None:
    with patch.object(lh, "download_history", return_value=pd.DataFrame()):
        rows = lh.fetch_all_history(["AAPL"], sleep=lambda _: None)
    assert rows == []


def test_fetch_all_history_normalizes_date_to_midnight() -> None:
    """Regression test: yfinance's daily-bar index carries the exchange's
    session-open time (e.g. 13:30:00 UTC for NYSE), not midnight - confirmed
    live 2026-09-27 to break Snowflake's DATE cast on COPY INTO, since
    (symbol, date) is meant to be one row per trading day, not a specific
    intraday moment."""
    exchange_open = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02 13:30:00", tz="UTC")],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "symbol": ["AAPL"],
        }
    )
    with patch.object(lh, "download_history", return_value=exchange_open):
        rows = lh.fetch_all_history(["AAPL"], sleep=lambda _: None)

    assert rows[0]["date"] == pd.Timestamp("2026-01-02 00:00:00", tz="UTC")


# ---------------------------------------------------------------------------
# Spark expression builders (cloud-free boundary check; real DataFrame
# semantics are verified separately, same convention as the ticks tests)
# ---------------------------------------------------------------------------
def test_spark_expression_builders_require_no_platform_clients() -> None:
    frame = MagicMock()
    with patch.object(lh, "F") as functions, patch.object(lh, "Window"):
        lh.rank_latest_per_symbol_date(frame)
        functions.col.assert_any_call("bronze_ingested_at")
        lh.project_historical(frame)
        frame.select.assert_called_with(
            "symbol", "date", "open", "high", "low", "close", "volume", "source"
        )
