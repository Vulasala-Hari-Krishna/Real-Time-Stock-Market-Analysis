"""Unit tests for dashboards/landing_reader.py (Live Data page's S3
landing/ticks/ reader)."""

import base64
import gzip
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from dashboards import landing_reader
from src.config.settings import Settings


def _envelope(tick: dict) -> bytes:
    """Build one NDJSON line the way raw_landing.py::encode_record does -
    the tick payload base64-encoded inside value_base64."""
    value = json.dumps(tick).encode("utf-8")
    line = json.dumps({"value_base64": base64.b64encode(value).decode("ascii")})
    return (line + "\n").encode("utf-8")


def _gzip_body(*ticks: dict) -> bytes:
    return gzip.compress(b"".join(_envelope(t) for t in ticks))


@pytest.fixture()
def settings() -> Settings:
    return Settings(
        raw_source_id="test-source",
        raw_topic="raw_stock_ticks",
        s3_bucket_name="test-bucket",
        aws_default_region="us-east-1",
    )


def test_decode_envelopes_extracts_the_tick_payload() -> None:
    body = _gzip_body(
        {
            "symbol": "AAPL",
            "price": 100.5,
            "volume": 10,
            "timestamp": "2026-01-01T00:00:00Z",
        }
    )
    rows = landing_reader._decode_envelopes(body)
    assert rows == [
        {
            "symbol": "AAPL",
            "price": 100.5,
            "volume": 10,
            "timestamp": "2026-01-01T00:00:00Z",
        }
    ]


def test_decode_envelopes_skips_unparsable_lines_without_raising() -> None:
    good = _envelope({"symbol": "AAPL", "price": 1.0, "volume": 1, "timestamp": "t"})
    bad = b"not json at all\n"
    body = gzip.compress(good + bad)
    rows = landing_reader._decode_envelopes(body)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


def test_load_live_ticks_reports_not_configured_when_source_id_is_blank() -> None:
    settings = Settings(raw_source_id="", s3_bucket_name="b")
    with patch.object(landing_reader, "get_settings", return_value=settings):
        df, status = landing_reader.load_live_ticks()
    assert status.source == "landing"
    assert status.ok is False
    assert "not configured" in status.message
    assert df.empty


def test_load_live_ticks_reports_failure_on_s3_error(settings: Settings) -> None:
    with patch.object(
        landing_reader, "get_settings", return_value=settings
    ), patch.object(
        landing_reader, "get_s3_client", side_effect=RuntimeError("no credentials")
    ):
        df, status = landing_reader.load_live_ticks()
    assert status.source == "landing"
    assert status.ok is False
    assert "no credentials" in status.message
    assert df.empty


def test_load_live_ticks_reports_empty_window_honestly(settings: Settings) -> None:
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": []}]
    client.get_paginator.return_value = paginator
    with patch.object(
        landing_reader, "get_settings", return_value=settings
    ), patch.object(landing_reader, "get_s3_client", return_value=client):
        df, status = landing_reader.load_live_ticks()
    assert status.ok is False
    assert "No ticks captured" in status.message
    assert df.empty


def test_load_live_ticks_happy_path_returns_sorted_ticks(settings: Settings) -> None:
    now = datetime.now(timezone.utc)
    body = _gzip_body(
        {"symbol": "MSFT", "price": 420.0, "volume": 5, "timestamp": (now).isoformat()},
        {
            "symbol": "AAPL",
            "price": 100.0,
            "volume": 3,
            "timestamp": (now - timedelta(seconds=30)).isoformat(),
        },
    )
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "landing/ticks/.../a.json.gz", "LastModified": now}]}
    ]
    client.get_paginator.return_value = paginator
    client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=body))
    }

    with patch.object(
        landing_reader, "get_settings", return_value=settings
    ), patch.object(landing_reader, "get_s3_client", return_value=client):
        df, status = landing_reader.load_live_ticks()

    assert status.ok is True
    assert len(df) == 2
    assert list(df["symbol"]) == ["AAPL", "MSFT"]  # sorted by timestamp
    assert status.as_of == df["timestamp"].max()
