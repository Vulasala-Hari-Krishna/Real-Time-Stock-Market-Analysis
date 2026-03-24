"""Integration test — S3 write / read round-trip.

Verifies that data can be written to S3 as Parquet, read back, and that
the schema and contents survive the round-trip. Uses ``unittest.mock``
to stub out real S3 calls so the test is hermetic.

Marked with ``pytest.mark.integration``.

Run with:

    pytest tests/integration -v -m integration
"""

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from src.common.s3_utils import (
    generate_s3_key,
    read_parquet_from_s3,
    upload_json_to_s3,
    upload_parquet_to_s3,
)


pytestmark = pytest.mark.integration


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_ohlcv_df() -> pd.DataFrame:
    """A small OHLCV DataFrame for round-trip testing."""
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
    """A sample JSON record for bronze upload testing."""
    return {
        "symbol": "AAPL",
        "price": 178.5,
        "volume": 52_000_000,
        "timestamp": "2024-03-15T14:30:00Z",
        "source": "alpha_vantage",
    }


# ── S3 key generation tests ────────────────────────────────────────────


class TestS3KeyGeneration:
    """Verify S3 key paths follow the medallion convention."""

    def test_bronze_key_format(self) -> None:
        key = generate_s3_key("bronze", "stock_ticks", date(2024, 3, 15), "AAPL")
        assert "bronze" in key
        assert "year=2024" in key
        assert "month=03" in key
        assert "AAPL" in key

    def test_silver_key_format(self) -> None:
        key = generate_s3_key("silver", "historical", date(2024, 3, 15), "MSFT")
        assert "silver" in key
        assert "MSFT" in key

    def test_gold_key_includes_layer(self) -> None:
        key = generate_s3_key("gold", "daily_summaries", date(2024, 3, 15))
        assert key.startswith("gold/")


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

        result = upload_json_to_s3(sample_json_data, "test-bucket", "bronze/test.json")
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

        result = upload_json_to_s3(sample_json_data, "test-bucket", "bronze/test.json")
        assert result is False


# ── Parquet upload tests ────────────────────────────────────────────────


class TestParquetUpload:
    """Test Parquet upload to S3 with mocked boto3."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_parquet_success(
        self,
        mock_get_client: MagicMock,
        sample_ohlcv_df: pd.DataFrame,
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        result = upload_parquet_to_s3(
            sample_ohlcv_df, "test-bucket", "silver/test.parquet"
        )
        assert result is True
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args
        # Verify Parquet bytes were written
        body = call_kwargs.kwargs.get("Body") or call_kwargs[1].get("Body")
        assert body is not None
        assert len(body) > 0

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_parquet_preserves_schema(
        self,
        mock_get_client: MagicMock,
        sample_ohlcv_df: pd.DataFrame,
    ) -> None:
        captured_body = None
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        def capture_put(**kwargs):
            nonlocal captured_body
            captured_body = kwargs.get("Body")

        mock_client.put_object.side_effect = capture_put

        upload_parquet_to_s3(sample_ohlcv_df, "test-bucket", "silver/test.parquet")

        # Read back the captured Parquet bytes to verify schema
        assert captured_body is not None
        table = pq.read_table(io.BytesIO(captured_body))
        schema_names = {f.name for f in table.schema}
        assert "symbol" in schema_names
        assert "close" in schema_names
        assert len(table) == 3


# ── Parquet read tests ──────────────────────────────────────────────────


class TestParquetRead:
    """Test reading Parquet back from S3."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_read_parquet_returns_dataframe(
        self,
        mock_get_client: MagicMock,
        sample_ohlcv_df: pd.DataFrame,
    ) -> None:
        # Create real Parquet bytes from the sample
        buf = io.BytesIO()
        sample_ohlcv_df.to_parquet(buf, index=False)
        parquet_bytes = buf.getvalue()

        mock_client = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = parquet_bytes
        mock_client.get_object.return_value = {"Body": mock_body}
        mock_get_client.return_value = mock_client

        result = read_parquet_from_s3("test-bucket", "silver/test.parquet")
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == list(sample_ohlcv_df.columns)

    @patch("src.common.s3_utils.get_s3_client")
    def test_read_parquet_missing_key_returns_none(
        self,
        mock_get_client: MagicMock,
    ) -> None:
        mock_client = MagicMock()
        mock_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject",
        )
        mock_get_client.return_value = mock_client

        result = read_parquet_from_s3("test-bucket", "silver/missing.parquet")
        assert result is None

    @patch("src.common.s3_utils.get_s3_client")
    def test_round_trip_preserves_data(
        self,
        mock_get_client: MagicMock,
        sample_ohlcv_df: pd.DataFrame,
    ) -> None:
        """Write → read round-trip preserves all values."""
        stored = {}
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        def store_put(**kwargs):
            stored["body"] = kwargs.get("Body")

        mock_client.put_object.side_effect = store_put

        upload_parquet_to_s3(sample_ohlcv_df, "test-bucket", "silver/rt.parquet")

        # Now read back
        mock_body = MagicMock()
        mock_body.read.return_value = stored["body"]
        mock_client.get_object.return_value = {"Body": mock_body}

        result = read_parquet_from_s3("test-bucket", "silver/rt.parquet")
        assert result is not None
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            sample_ohlcv_df.reset_index(drop=True),
        )


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
