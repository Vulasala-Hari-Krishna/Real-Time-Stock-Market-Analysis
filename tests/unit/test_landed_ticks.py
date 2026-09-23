"""Contracts for Databricks processing of consumer-landed quote envelopes."""

import base64
import json

import pytest

from src.batch.landed_ticks import normalize_envelope


def test_normalize_envelope_reuses_stock_tick_validation() -> None:
    """Decode bytes without confusing capture time with an exchange trade."""
    payload = {
        "symbol": "aapl",
        "price": 150.5,
        "volume": 1000,
        "timestamp": "2026-09-23T12:00:00+00:00",
        "source": "alpha_vantage",
    }
    envelope = {
        "schema_version": 1,
        "source_id": "local-v1",
        "topic": "raw_stock_ticks",
        "partition": 0,
        "offset": 7,
        "kafka_timestamp": None,
        "ingested_at": "2026-09-23T12:01:00+00:00",
        "key_base64": None,
        "value_base64": base64.b64encode(json.dumps(payload).encode()).decode(),
        "headers": [],
    }

    result = normalize_envelope(json.dumps(envelope))

    assert result["rejection_reason"] is None
    assert result["symbol"] == "AAPL"
    assert result["reported_volume"] == 1000
    assert result["quote_timestamp"] == "2026-09-23T12:00:00+00:00"
    assert result["offset"] == 7


@pytest.fixture()
def envelope() -> dict:
    """A representative raw envelope, independent of Kafka and AWS."""
    return {
        "schema_version": 1,
        "source_id": "local-v1",
        "topic": "raw_stock_ticks",
        "partition": 0,
        "offset": 1,
        "kafka_timestamp": None,
        "ingested_at": "2026-09-23T12:01:00Z",
        "key_base64": None,
        "value_base64": base64.b64encode(
            json.dumps(
                {
                    "symbol": "AAPL",
                    "price": 150.0,
                    "volume": 1000,
                    "timestamp": "2026-09-23T12:00:00Z",
                    "source": "alpha_vantage",
                }
            ).encode()
        ).decode(),
        "headers": [],
    }


@pytest.mark.parametrize("raw", ["bad-json", "null", "[]", "{}"])
def test_malformed_envelopes_are_quarantined(raw: str) -> None:
    assert normalize_envelope(raw)["rejection_reason"] == "invalid_envelope"


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("offset", -1),
        ("offset", 2**63),
        ("partition", "0"),
        ("source_id", "../bad"),
        ("ingested_at", "2026-09-23T12:00:00"),
        ("headers", {}),
    ],
)
def test_invalid_transport_contract(envelope: dict, field: str, value: object) -> None:
    envelope[field] = value
    assert (
        normalize_envelope(json.dumps(envelope))["rejection_reason"]
        == "invalid_envelope"
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"price": 0},
        {"price": float("inf")},
        {"price": float("nan")},
        {"volume": -1},
        {"volume": 2**63},
        {"volume": "100"},
        {"symbol": " "},
        {"timestamp": "2026-09-23T12:00:00"},
        {"source": " "},
    ],
)
def test_invalid_quotes_are_quarantined(envelope: dict, payload: dict) -> None:
    tick = json.loads(base64.b64decode(envelope["value_base64"]))
    tick.update(payload)
    envelope["value_base64"] = base64.b64encode(json.dumps(tick).encode()).decode()
    assert (
        normalize_envelope(json.dumps(envelope))["rejection_reason"] == "invalid_tick"
    )


@pytest.mark.parametrize("payload", [b"not-json", b"\xff", b"null"])
def test_invalid_payload_bytes(envelope: dict, payload: bytes) -> None:
    envelope["value_base64"] = base64.b64encode(payload).decode()
    assert (
        normalize_envelope(json.dumps(envelope))["rejection_reason"] == "invalid_tick"
    )


def test_base64_headers_tombstone_and_replay_fingerprint(envelope: dict) -> None:
    original = normalize_envelope(json.dumps(envelope))
    envelope["ingested_at"] = "2026-09-24T12:00:00Z"
    assert (
        normalize_envelope(json.dumps(envelope))["payload_sha256"]
        == original["payload_sha256"]
    )
    envelope["headers"] = [{"key": "trace", "value_base64": "%%%"}]
    assert (
        normalize_envelope(json.dumps(envelope))["rejection_reason"] == "invalid_base64"
    )
    envelope["headers"] = [{"key": "trace", "value_base64": None}]
    envelope["value_base64"] = None
    assert normalize_envelope(json.dumps(envelope))["rejection_reason"] == "tombstone"
