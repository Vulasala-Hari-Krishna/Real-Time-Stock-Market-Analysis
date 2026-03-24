"""Unit tests for S3 utility functions."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from src.common.s3_utils import (
    generate_s3_key,
    read_parquet_from_s3,
    upload_json_to_s3,
    upload_parquet_to_s3,
)


# ---------------------------------------------------------------------------
# generate_s3_key
# ---------------------------------------------------------------------------
class TestGenerateS3Key:
    """Tests for S3 key generation."""

    def test_bronze_key_with_symbol(self) -> None:
        """Generates a bronze-layer key with .json extension."""
        dt = datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = generate_s3_key("bronze", "stock_ticks", partition_date=dt, symbol="AAPL")

        assert (
            key
            == "bronze/stock_ticks/year=2024/month=03/day=15/AAPL_20240315T143000Z.json"
        )

    def test_silver_key_with_symbol(self) -> None:
        """Generates a silver-layer key with .parquet extension."""
        dt = datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = generate_s3_key("silver", "daily_ohlcv", partition_date=dt, symbol="MSFT")

        assert (
            key
            == "silver/daily_ohlcv/year=2024/month=03/day=15/MSFT_20240315T143000Z.parquet"
        )

    def test_gold_key_without_symbol(self) -> None:
        """Generates a gold-layer key without symbol in filename."""
        dt = datetime(2024, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
        key = generate_s3_key("gold", "daily_summary", partition_date=dt)

        assert (
            key
            == "gold/daily_summary/year=2024/month=12/day=01/20241201T000000Z.parquet"
        )

    def test_default_date_uses_now(self) -> None:
        """Uses current UTC time when no partition_date is given."""
        key = generate_s3_key("bronze", "stock_ticks", symbol="AAPL")

        assert key.startswith("bronze/stock_ticks/year=")
        assert "AAPL_" in key
        assert key.endswith(".json")

    @pytest.mark.parametrize(
        "layer,expected_ext",
        [
            ("bronze", ".json"),
            ("silver", ".parquet"),
            ("gold", ".parquet"),
        ],
    )
    def test_extension_per_layer(self, layer: str, expected_ext: str) -> None:
        """Bronze uses .json, silver/gold use .parquet."""
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        key = generate_s3_key(layer, "test_table", partition_date=dt)
        assert key.endswith(expected_ext)

    def test_partitioning_structure(self) -> None:
        """Key contains year, month, day partition segments."""
        dt = datetime(2024, 7, 4, 12, 0, 0, tzinfo=timezone.utc)
        key = generate_s3_key("silver", "data", partition_date=dt)

        assert "year=2024" in key
        assert "month=07" in key
        assert "day=04" in key


# ---------------------------------------------------------------------------
# upload_json_to_s3
# ---------------------------------------------------------------------------
class TestUploadJsonToS3:
    """Tests for JSON upload to S3."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_successful_upload(self, mock_get_client: MagicMock) -> None:
        """Uploads JSON data and returns True."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        result = upload_json_to_s3(
            data={"key": "value"},
            bucket="test-bucket",
            key="test/data.json",
        )

        assert result is True
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "test/data.json"
        assert call_kwargs["ContentType"] == "application/json"

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_failure(self, mock_get_client: MagicMock) -> None:
        """Returns False when S3 put fails."""
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        mock_get_client.return_value = mock_client

        result = upload_json_to_s3(
            data={"key": "value"},
            bucket="test-bucket",
            key="test/data.json",
        )

        assert result is False

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_list_data(self, mock_get_client: MagicMock) -> None:
        """Can upload a list as JSON."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        result = upload_json_to_s3(
            data=[1, 2, 3],
            bucket="test-bucket",
            key="test/list.json",
        )

        assert result is True


# ---------------------------------------------------------------------------
# upload_parquet_to_s3
# ---------------------------------------------------------------------------
class TestUploadParquetToS3:
    """Tests for Parquet upload to S3."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_successful_upload(self, mock_get_client: MagicMock) -> None:
        """Uploads a DataFrame as Parquet and returns True."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        df = pd.DataFrame({"symbol": ["AAPL"], "price": [178.50]})
        result = upload_parquet_to_s3(
            df=df,
            bucket="test-bucket",
            key="test/data.parquet",
        )

        assert result is True
        mock_client.put_object.assert_called_once()

    @patch("src.common.s3_utils.get_s3_client")
    def test_upload_failure(self, mock_get_client: MagicMock) -> None:
        """Returns False when S3 put fails."""
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Server Error"}},
            "PutObject",
        )
        mock_get_client.return_value = mock_client

        df = pd.DataFrame({"a": [1]})
        result = upload_parquet_to_s3(
            df=df,
            bucket="test-bucket",
            key="test/data.parquet",
        )

        assert result is False


# ---------------------------------------------------------------------------
# read_parquet_from_s3
# ---------------------------------------------------------------------------
class TestReadParquetFromS3:
    """Tests for reading Parquet from S3."""

    @patch("src.common.s3_utils.get_s3_client")
    def test_successful_read(self, mock_get_client: MagicMock) -> None:
        """Reads a Parquet file from S3 into a DataFrame."""
        import io

        # Create a real parquet buffer
        df_original = pd.DataFrame(
            {"symbol": ["AAPL", "MSFT"], "price": [178.5, 400.0]}
        )
        buffer = io.BytesIO()
        df_original.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        mock_client = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = buffer.getvalue()
        mock_client.get_object.return_value = {"Body": mock_body}
        mock_get_client.return_value = mock_client

        result = read_parquet_from_s3(bucket="test-bucket", key="test/data.parquet")

        assert result is not None
        assert len(result) == 2
        assert list(result.columns) == ["symbol", "price"]

    @patch("src.common.s3_utils.get_s3_client")
    def test_read_failure(self, mock_get_client: MagicMock) -> None:
        """Returns None when S3 get fails."""
        mock_client = MagicMock()
        mock_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_get_client.return_value = mock_client

        result = read_parquet_from_s3(bucket="test-bucket", key="missing.parquet")

        assert result is None
