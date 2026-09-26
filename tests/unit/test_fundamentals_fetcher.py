"""Unit tests for the local fundamentals fetcher (src/producers/fundamentals_fetcher.py)."""

import gzip
import io
import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.producers import fundamentals_fetcher as fetcher


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
        record = fetcher.fetch_one("AAPL")

    assert record is not None
    assert record.symbol == "AAPL"
    assert record.market_cap == 2.8e12
    assert record.sector == "Technology"
    assert record.retrieved_at.utcoffset() is not None


def test_fetch_one_returns_none_when_yfinance_always_raises() -> None:
    sleeps: list[float] = []
    with patch("yfinance.Ticker", side_effect=RuntimeError("429 Too Many Requests")):
        assert fetcher.fetch_one("AAPL", sleep=sleeps.append) is None
    # Retries with backoff before giving up - never blocks a real test run.
    assert sleeps == list(fetcher.RETRY_DELAYS_SECONDS)


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
        record = fetcher.fetch_one("AAPL", sleep=sleeps.append)

    assert record is not None
    assert record.symbol == "AAPL"
    assert attempts["count"] == 3
    # Two retries needed (first two attempts failed) - only their delays.
    assert sleeps == list(fetcher.RETRY_DELAYS_SECONDS[:2])


def test_fetch_one_returns_none_on_invalid_fields() -> None:
    with patch("yfinance.Ticker") as mock_ticker:
        mock_ticker.return_value.info = _fake_ticker_info(fiftyTwoWeekHigh=-1)
        assert fetcher.fetch_one("AAPL") is None


# ---------------------------------------------------------------------------
# build_batch
# ---------------------------------------------------------------------------
def test_build_batch_produces_one_gzip_ndjson_line_per_record() -> None:
    with patch("yfinance.Ticker") as mock_ticker:
        mock_ticker.return_value.info = _fake_ticker_info()
        records = [fetcher.fetch_one("AAPL"), fetcher.fetch_one("MSFT")]

    body = fetcher.build_batch(records, "batch-1")
    lines = gzip.decompress(body).decode().strip().split("\n")

    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert {p["symbol"] for p in parsed} == {"AAPL", "MSFT"}
    assert all(p["extraction_id"] == "batch-1" for p in parsed)


# ---------------------------------------------------------------------------
# upload_batch
# ---------------------------------------------------------------------------
def test_upload_batch_writes_new_object() -> None:
    client = MagicMock()
    client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "GetObject"
    )

    fetcher.upload_batch(client, "bucket", "key", b"body")

    client.put_object.assert_called_once()
    assert client.put_object.call_args.kwargs["Body"] == b"body"


def test_upload_batch_is_a_noop_when_identical_object_exists() -> None:
    client = MagicMock()
    client.get_object.return_value = {"Body": io.BytesIO(b"body")}

    fetcher.upload_batch(client, "bucket", "key", b"body")

    client.put_object.assert_not_called()


def test_upload_batch_rejects_conflicting_existing_object() -> None:
    client = MagicMock()
    client.get_object.return_value = {"Body": io.BytesIO(b"different")}

    with pytest.raises(ValueError, match="differs"):
        fetcher.upload_batch(client, "bucket", "key", b"body")


# ---------------------------------------------------------------------------
# run_fetch
# ---------------------------------------------------------------------------
@patch("src.producers.fundamentals_fetcher.get_s3_client")
@patch("src.producers.fundamentals_fetcher.upload_batch")
def test_run_fetch_uploads_a_batch_for_successful_symbols(
    mock_upload: MagicMock, mock_get_client: MagicMock
) -> None:
    with patch("yfinance.Ticker") as mock_ticker:
        mock_ticker.return_value.info = _fake_ticker_info()
        result = fetcher.run_fetch(
            "test-bucket", symbols=["AAPL", "MSFT"], sleep=lambda _: None
        )

    assert result == {"fetched": 2, "skipped": 0}
    mock_upload.assert_called_once()
    call_args = mock_upload.call_args.args
    assert call_args[1] == "test-bucket"
    assert call_args[2].startswith("landing/fundamentals/extraction_id=")


@patch("src.producers.fundamentals_fetcher.get_s3_client")
@patch("src.producers.fundamentals_fetcher.upload_batch")
def test_run_fetch_skips_failed_symbols_but_uploads_the_rest(
    mock_upload: MagicMock, mock_get_client: MagicMock
) -> None:
    def fake_ticker(symbol: str) -> MagicMock:
        mock = MagicMock()
        if symbol == "BAD":
            raise RuntimeError("boom")
        mock.info = _fake_ticker_info()
        return mock

    with patch("yfinance.Ticker", side_effect=fake_ticker):
        result = fetcher.run_fetch(
            "test-bucket", symbols=["AAPL", "BAD"], sleep=lambda _: None
        )

    assert result == {"fetched": 1, "skipped": 1}
    mock_upload.assert_called_once()


@patch("src.producers.fundamentals_fetcher.get_s3_client")
def test_run_fetch_refuses_to_upload_when_everything_fails(
    mock_get_client: MagicMock,
) -> None:
    with patch("yfinance.Ticker", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="refusing to upload an empty batch"):
            fetcher.run_fetch("test-bucket", symbols=["AAPL"], sleep=lambda _: None)
