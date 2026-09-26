"""Unit tests for the dashboard's Snowflake historical loader
(dashboards/snowflake_loader.py, R5)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dashboards import snowflake_loader as loader


@pytest.fixture(autouse=True)
def _clear_streamlit_cache():
    """st.cache_data persists across calls within a process; clear it
    before/after every test so mocked _connect behavior in one test can't
    leak a cached DataFrame into the next."""
    loader._fetch_from_snowflake.clear()
    yield
    loader._fetch_from_snowflake.clear()


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


# ---------------------------------------------------------------------------
# _generate_demo_daily_quote_summary
# ---------------------------------------------------------------------------
def test_generate_demo_data_has_expected_columns_and_is_deterministic() -> None:
    first = loader._generate_demo_daily_quote_summary()
    second = loader._generate_demo_daily_quote_summary()

    assert list(first.columns) == loader.DASHBOARD_COLUMNS
    assert not first.empty
    assert set(first["symbol"].unique()) == set(loader.SYMBOLS)
    # loaded_at is wall-clock "when this demo batch was generated", not
    # part of the deterministic price/volume series - compare everything else.
    compare_columns = [c for c in loader.DASHBOARD_COLUMNS if c != "loaded_at"]
    pd.testing.assert_frame_equal(first[compare_columns], second[compare_columns])


# ---------------------------------------------------------------------------
# load_daily_quote_summary
# ---------------------------------------------------------------------------
@patch("dashboards.snowflake_loader._connect")
def test_load_daily_quote_summary_returns_snowflake_status_on_success(
    mock_connect: MagicMock,
) -> None:
    columns = loader.DASHBOARD_COLUMNS
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
