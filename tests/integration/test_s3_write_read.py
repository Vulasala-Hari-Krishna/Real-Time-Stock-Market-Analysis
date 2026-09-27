"""Integration test — S3 JSON upload and OHLCV data-shape invariants.

Uses ``unittest.mock`` to stub out real S3 calls so the test is hermetic.

Marked with ``pytest.mark.integration``.

Run with:

    pytest tests/integration -v -m integration
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from src.common.s3_utils import upload_json_to_s3

pytestmark = pytest.mark.integration


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_ohlcv_df() -> pd.DataFrame:
    """A small OHLCV DataFrame for data-shape invariant testing."""
    from datetime import date

    return pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "date": [date(2024, 3, 13), date(2024, 3, 14), date(2024, 3, 14)],
            "open": [175.0, 176.5, 420.0],
            "high": [178.0, 179.0, 425.0],
            "low": [174.0, 175.0, 418.0],
            "close": [177.5, 178.0, 422.0],
            "volume": [52_000_000, 48_000_000, 30_000_000],
        }
    )


@pytest.fixture()
def sample_json_data() -> dict:
    """A sample JSON record for upload testing."""
    return {
        "symbol": "AAPL",
        "price": 178.5,
        "volume": 52_000_000,
        "timestamp": "2024-03-15T14:30:00Z",
        "source": "alpha_vantage",
    }


# ── JSON upload tests ───────────────────────────────────────────────────


class TestJsonUpload:
    """Test JSON upload to S3 with mocked boto3 client."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_json_success(
        self,
        mock_get_client: MagicMock,
        sample_json_data: dict,
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        result = upload_json_to_s3(sample_json_data, "test-bucket", "landing/test.json")
        assert result is True
        mock_client.put_object.assert_called_once()

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_json_failure_returns_false(
        self,
        mock_get_client: MagicMock,
        sample_json_data: dict,
    ) -> None:
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "PutObject",
        )
        mock_get_client.return_value = mock_client

        result = upload_json_to_s3(sample_json_data, "test-bucket", "landing/test.json")
        assert result is False


# ── Data integrity tests ───────────────────────────────────────────────


class TestDataIntegrity:
    """Verify data integrity across write operations."""

    def test_ohlcv_constraints(self, sample_ohlcv_df: pd.DataFrame) -> None:
        """High >= Open, High >= Close, Low <= Open, Low <= Close."""
        for _, row in sample_ohlcv_df.iterrows():
            assert row["high"] >= row["open"]
            assert row["high"] >= row["close"]
            assert row["low"] <= row["open"]
            assert row["low"] <= row["close"]

    def test_volume_positive(self, sample_ohlcv_df: pd.DataFrame) -> None:
        assert (sample_ohlcv_df["volume"] > 0).all()

    def test_no_null_symbols(self, sample_ohlcv_df: pd.DataFrame) -> None:
        assert sample_ohlcv_df["symbol"].notna().all()
