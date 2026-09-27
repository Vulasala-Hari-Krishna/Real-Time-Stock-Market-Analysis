"""Unit tests for the dashboard's Snowflake historical loader
(dashboards/snowflake_loader.py, R5)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dashboards import local_cache, snowflake_loader as loader

# Column order the daily_quote_summary demo generator/SERVING table use -
# no longer exported as a module constant now that snowflake_loader.py is
# generic across datasets, so the tests that need a fixed row shape define
# it locally.
DAILY_QUOTE_SUMMARY_COLUMNS = [
    "provider",
    "symbol",
    "capture_date_utc",
    "first_observed_price",
    "highest_observed_price",
    "lowest_observed_price",
    "last_observed_price",
    "last_reported_volume",
    "quote_count",
    "first_quote_at",
    "last_quote_at",
    "observed_change_pct",
    "bronze_version",
    "loaded_batch_id",
    "loaded_at",
]


@pytest.fixture(autouse=True)
def _clear_streamlit_cache():
    """st.cache_data persists across calls within a process; clear it
    before/after every test so mocked _connect behavior in one test can't
    leak a cached DataFrame into the next."""
    loader._fetch_from_snowflake.clear()
    yield
    loader._fetch_from_snowflake.clear()


@pytest.fixture(autouse=True)
def _isolate_local_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Never touch the real .state/dashboard-cache directory in tests."""
    monkeypatch.setattr(local_cache, "CACHE_DIR", tmp_path / "dashboard-cache")


# ---------------------------------------------------------------------------
# _private_key_der (real cryptography, no mocks)
# ---------------------------------------------------------------------------
def test_private_key_der_roundtrips_a_real_key() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    der = loader._private_key_der(pem, None)
    reloaded = serialization.load_der_private_key(der, password=None)
    assert reloaded.key_size == 2048


def test_private_key_der_accepts_escaped_newlines_from_a_single_line_env_var() -> None:
    """Regression test: a local .env file cannot reliably hold a real
    multi-line value, so the documented local setup stores the key as one
    line with literal "\\n" escapes instead of real newlines."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    escaped_single_line = pem.replace("\n", "\\n")

    der = loader._private_key_der(escaped_single_line, None)
    reloaded = serialization.load_der_private_key(der, password=None)
    assert reloaded.key_size == 2048


# ---------------------------------------------------------------------------
# _generate_demo_daily_quote_summary
# ---------------------------------------------------------------------------
def test_generate_demo_data_has_expected_columns_and_is_deterministic() -> None:
    first = loader._generate_demo_daily_quote_summary()
    second = loader._generate_demo_daily_quote_summary()

    assert list(first.columns) == DAILY_QUOTE_SUMMARY_COLUMNS
    assert not first.empty
    assert set(first["symbol"].unique()) == set(loader.SYMBOLS)
    # loaded_at is wall-clock "when this demo batch was generated", not
    # part of the deterministic price/volume series - compare everything else.
    compare_columns = [c for c in DAILY_QUOTE_SUMMARY_COLUMNS if c != "loaded_at"]
    pd.testing.assert_frame_equal(first[compare_columns], second[compare_columns])


# ---------------------------------------------------------------------------
# load_daily_quote_summary
# ---------------------------------------------------------------------------
@patch("dashboards.snowflake_loader._connect")
def test_load_daily_quote_summary_returns_snowflake_status_on_success(
    mock_connect: MagicMock,
) -> None:
    columns = DAILY_QUOTE_SUMMARY_COLUMNS
    rows = [
        (
            "alpha_vantage",
            "AAPL",
            "2026-01-01",
            150.0,
            151.0,
            149.0,
            150.5,
            1000,
            5,
            "2026-01-01T00:00:00Z",
            "2026-01-01T08:00:00Z",
            0.33,
            5,
            "batch-1",
            pd.Timestamp("2026-01-02T00:00:00Z"),
        ),
    ]
    mock_cursor = MagicMock()
    mock_cursor.description = [(c,) for c in columns]
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    df, status = loader.load_daily_quote_summary()

    assert status.source == "snowflake"
    assert status.ok is True
    assert status.batch_id == "batch-1"
    assert status.as_of == pd.Timestamp("2026-01-02T00:00:00Z")
    assert len(df) == 1
    mock_conn.close.assert_called_once()


@patch("dashboards.snowflake_loader._connect")
def test_load_daily_quote_summary_falls_back_to_demo_on_connection_failure(
    mock_connect: MagicMock,
) -> None:
    mock_connect.side_effect = RuntimeError("no credentials")

    df, status = loader.load_daily_quote_summary()

    assert status.source == "demo"
    assert status.ok is False
    assert "no credentials" in status.message
    assert not df.empty
    assert set(df["symbol"].unique()) == set(loader.SYMBOLS)


@patch("dashboards.snowflake_loader._connect")
def test_load_daily_quote_summary_closes_connection_even_on_query_error(
    mock_connect: MagicMock,
) -> None:
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = RuntimeError("query failed")
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    df, status = loader.load_daily_quote_summary()

    assert status.source == "demo"
    assert status.ok is False
    mock_conn.close.assert_called_once()


def _mock_connect_with_one_row(mock_connect: MagicMock, batch_id: str) -> None:
    columns = DAILY_QUOTE_SUMMARY_COLUMNS
    row = (
        "alpha_vantage",
        "AAPL",
        "2026-01-01",
        150.0,
        151.0,
        149.0,
        150.5,
        1000,
        5,
        "2026-01-01T00:00:00Z",
        "2026-01-01T08:00:00Z",
        0.33,
        5,
        batch_id,
        pd.Timestamp("2026-01-02T00:00:00Z"),
    )
    mock_cursor = MagicMock()
    mock_cursor.description = [(c,) for c in columns]
    mock_cursor.fetchall.return_value = [row]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn


# ---------------------------------------------------------------------------
# R7: durable local cache interaction
# ---------------------------------------------------------------------------
@patch("dashboards.snowflake_loader._connect")
def test_successful_load_saves_a_local_cache_snapshot(
    mock_connect: MagicMock,
) -> None:
    _mock_connect_with_one_row(mock_connect, batch_id="batch-1")

    loader.load_daily_quote_summary()

    cached = local_cache.load_snapshot("daily_quote_summary")
    assert cached is not None
    cached_df, metadata = cached
    assert len(cached_df) == 1
    assert metadata["batch_id"] == "batch-1"


@patch("dashboards.snowflake_loader._connect")
def test_failed_load_prefers_cached_data_over_demo_data(
    mock_connect: MagicMock,
) -> None:
    # First call succeeds and populates the cache.
    _mock_connect_with_one_row(mock_connect, batch_id="batch-1")
    loader.load_daily_quote_summary()
    loader._fetch_from_snowflake.clear()

    # Second call fails - should fall back to the cache, not demo data.
    mock_connect.side_effect = RuntimeError("connection lost")
    df, status = loader.load_daily_quote_summary()

    assert status.source == "cache"
    assert status.ok is False
    assert status.batch_id == "batch-1"
    assert "connection lost" in status.message
    assert len(df) == 1


# ---------------------------------------------------------------------------
# Generalization across the other registered datasets - one representative
# dataset is enough to prove the registry-driven mechanism works; every
# other dataset goes through the identical load_dataset() code path.
# ---------------------------------------------------------------------------
@patch("dashboards.snowflake_loader._connect")
def test_load_daily_summaries_queries_its_own_serving_table(
    mock_connect: MagicMock,
) -> None:
    mock_cursor = MagicMock()
    mock_cursor.description = [("SYMBOL",), ("DATE",), ("CLOSE",)]
    mock_cursor.fetchall.return_value = [("AAPL", "2026-01-01", 150.0)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    df, status = loader.load_daily_summaries()

    mock_cursor.execute.assert_called_once_with("SELECT * FROM DAILY_SUMMARIES")
    assert status.source == "snowflake"
    assert status.ok is True
    assert list(df.columns) == ["symbol", "date", "close"]
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


@patch("dashboards.snowflake_loader._connect")
def test_load_daily_summaries_falls_back_to_its_own_demo_shape(
    mock_connect: MagicMock,
) -> None:
    mock_connect.side_effect = RuntimeError("unavailable")

    df, status = loader.load_daily_summaries()

    assert status.source == "demo"
    assert not df.empty
    assert "sector" in df.columns  # daily_summaries-specific demo shape


@patch("dashboards.snowflake_loader._connect")
def test_cache_write_failure_does_not_break_a_successful_load(
    mock_connect: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A real disk/permissions failure inside save_snapshot's own writes -
    # save_snapshot is expected to catch this itself (see test_local_cache.py),
    # this test confirms the caller never depends on that in a way that
    # would break if it didn't.
    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(local_cache.Path, "mkdir", _boom)
    _mock_connect_with_one_row(mock_connect, batch_id="batch-1")

    df, status = loader.load_daily_quote_summary()

    assert status.source == "snowflake"
    assert status.ok is True
    assert len(df) == 1
