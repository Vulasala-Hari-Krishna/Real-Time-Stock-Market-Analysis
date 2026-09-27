"""Unit tests for S3 utility functions."""

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from src.common.s3_utils import upload_json_to_s3


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
