"""Contracts for Databricks processing of landed fundamentals snapshots (R6)."""

import json

import pytest

from src.batch.landed_fundamentals import normalize_fundamentals_record


@pytest.fixture()
def record() -> dict:
    """A representative landed fundamentals line."""
    return {
        "extraction_id": "20260101T000000Z-aaaaaaaa",
        "symbol": "AAPL",
        "retrieved_at": "2026-01-01T00:00:00+00:00",
        "market_cap": 2.8e12,
        "pe_ratio": 28.5,
        "forward_pe": 26.0,
        "dividend_yield": 0.005,
        "eps": 6.25,
        "beta": 1.2,
        "fifty_two_week_high": 199.6,
        "fifty_two_week_low": 143.9,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }


def test_normalize_fundamentals_record_accepts_a_valid_line(record: dict) -> None:
    result = normalize_fundamentals_record(json.dumps(record))

    assert result["rejection_reason"] is None
    assert result["extraction_id"] == "20260101T000000Z-aaaaaaaa"
    assert result["symbol"] == "AAPL"
    assert result["retrieved_at"] == "2026-01-01T00:00:00+00:00"
    assert result["market_cap"] == 2.8e12
    assert result["sector"] == "Technology"


@pytest.mark.parametrize("raw", ["bad-json", "null", "[]", "42"])
def test_malformed_lines_are_quarantined(raw: str) -> None:
    assert normalize_fundamentals_record(raw)["rejection_reason"] == "invalid_record"


def test_missing_extraction_id_is_quarantined(record: dict) -> None:
    del record["extraction_id"]
    assert (
        normalize_fundamentals_record(json.dumps(record))["rejection_reason"]
        == "invalid_record"
    )


@pytest.mark.parametrize("bad_id", ["", "../bad", "has spaces", "a" * 65])
def test_invalid_extraction_id_is_quarantined(record: dict, bad_id: str) -> None:
    record["extraction_id"] = bad_id
    assert (
        normalize_fundamentals_record(json.dumps(record))["rejection_reason"]
        == "invalid_record"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", ""),
        ("market_cap", -1),
        ("fifty_two_week_high", 0),
        ("fifty_two_week_low", -5),
    ],
)
def test_invalid_fundamental_fields_are_quarantined(
    record: dict, field: str, value: object
) -> None:
    record[field] = value
    assert (
        normalize_fundamentals_record(json.dumps(record))["rejection_reason"]
        == "invalid_fundamental"
    )


def test_naive_retrieved_at_is_quarantined(record: dict) -> None:
    record["retrieved_at"] = "2026-01-01T00:00:00"
    assert (
        normalize_fundamentals_record(json.dumps(record))["rejection_reason"]
        == "invalid_fundamental"
    )


def test_optional_fields_may_be_null(record: dict) -> None:
    for field in ("market_cap", "pe_ratio", "sector", "industry", "beta"):
        record[field] = None
    result = normalize_fundamentals_record(json.dumps(record))
    assert result["rejection_reason"] is None
    assert result["market_cap"] is None
    assert result["sector"] is None
