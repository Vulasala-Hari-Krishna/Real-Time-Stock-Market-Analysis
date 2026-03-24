"""Unit tests for the Kafka stock producer."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectionError, Timeout

from src.common.schemas import StockTick
from src.config.settings import Settings
from src.producers.stock_producer import (
    ALPHA_VANTAGE_BASE_URL,
    KAFKA_TOPIC,
    backup_to_s3,
    fetch_global_quote,
    parse_global_quote,
    publish_tick,
    run_producer,
)


# ---------------------------------------------------------------------------
# Sample API response fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def valid_api_response() -> dict:
    """A valid Alpha Vantage GLOBAL_QUOTE response."""
    return {
        "Global Quote": {
            "01. symbol": "AAPL",
            "02. open": "175.0000",
            "03. high": "180.0000",
            "04. low": "174.5000",
            "05. price": "178.5000",
            "06. volume": "52000000",
            "07. latest trading day": "2024-03-15",
            "08. previous close": "176.0000",
            "09. change": "2.5000",
            "10. change percent": "1.4205%",
        }
    }


@pytest.fixture()
def empty_api_response() -> dict:
    """An empty Global Quote response (rate limited or invalid symbol)."""
    return {"Global Quote": {}}


@pytest.fixture()
def test_settings() -> Settings:
    """Settings configured for testing."""
    return Settings(
        alpha_vantage_api_key="test_key",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        s3_bucket_name="test-bucket",
        kafka_broker="localhost:9092",
        run_pipeline=True,
        max_iterations=1,
        poll_interval_seconds=0,
        environment="dev",
    )


# ---------------------------------------------------------------------------
# fetch_global_quote
# ---------------------------------------------------------------------------
class TestFetchGlobalQuote:
    """Tests for the Alpha Vantage API fetching function."""

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_successful_fetch(
        self, mock_sleep: MagicMock, mock_get: MagicMock, valid_api_response: dict
    ) -> None:
        """Successfully fetches a valid quote."""
        mock_response = MagicMock()
        mock_response.json.return_value = valid_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_global_quote("AAPL", "test_key")

        assert result is not None
        assert "Global Quote" in result
        mock_get.assert_called_once()

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_empty_response_retries(
        self, mock_sleep: MagicMock, mock_get: MagicMock, empty_api_response: dict
    ) -> None:
        """Retries on empty Global Quote and returns None after max retries."""
        mock_response = MagicMock()
        mock_response.json.return_value = empty_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_global_quote("INVALID", "test_key")

        assert result is None
        assert mock_get.call_count == 3  # MAX_RETRIES

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_connection_error_retries(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Retries on connection errors and returns None after max retries."""
        mock_get.side_effect = ConnectionError("Connection refused")

        result = fetch_global_quote("AAPL", "test_key")

        assert result is None
        assert mock_get.call_count == 3

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_timeout_retries(self, mock_sleep: MagicMock, mock_get: MagicMock) -> None:
        """Retries on timeouts."""
        mock_get.side_effect = Timeout("Request timed out")

        result = fetch_global_quote("AAPL", "test_key")

        assert result is None
        assert mock_get.call_count == 3

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_retry_then_success(
        self,
        mock_sleep: MagicMock,
        mock_get: MagicMock,
        valid_api_response: dict,
    ) -> None:
        """Succeeds on second attempt after first failure."""
        mock_fail = MagicMock()
        mock_fail.json.return_value = {"Global Quote": {}}
        mock_fail.raise_for_status.return_value = None

        mock_success = MagicMock()
        mock_success.json.return_value = valid_api_response
        mock_success.raise_for_status.return_value = None

        mock_get.side_effect = [mock_fail, mock_success]

        result = fetch_global_quote("AAPL", "test_key")

        assert result is not None
        assert mock_get.call_count == 2

    @patch("src.producers.stock_producer.requests.get")
    @patch("src.producers.stock_producer.time.sleep")
    def test_api_params(
        self, mock_sleep: MagicMock, mock_get: MagicMock, valid_api_response: dict
    ) -> None:
        """Verifies correct API parameters are sent."""
        mock_response = MagicMock()
        mock_response.json.return_value = valid_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetch_global_quote("MSFT", "my_api_key")

        mock_get.assert_called_once_with(
            ALPHA_VANTAGE_BASE_URL,
            params={
                "function": "GLOBAL_QUOTE",
                "symbol": "MSFT",
                "apikey": "my_api_key",
            },
            timeout=10,
        )


# ---------------------------------------------------------------------------
# parse_global_quote
# ---------------------------------------------------------------------------
class TestParseGlobalQuote:
    """Tests for parsing Alpha Vantage responses into StockTick."""

    def test_valid_parse(self, valid_api_response: dict) -> None:
        """Parses a valid response into a StockTick."""
        tick = parse_global_quote("AAPL", valid_api_response)

        assert tick is not None
        assert tick.symbol == "AAPL"
        assert tick.price == 178.50
        assert tick.volume == 52_000_000
        assert tick.source == "alpha_vantage"

    def test_missing_key(self) -> None:
        """Returns None when response is missing expected keys."""
        bad_data = {"Global Quote": {"01. symbol": "AAPL"}}
        tick = parse_global_quote("AAPL", bad_data)
        assert tick is None

    def test_invalid_price(self) -> None:
        """Returns None when price is not a valid float."""
        bad_data = {
            "Global Quote": {
                "05. price": "not_a_number",
                "06. volume": "1000",
            }
        }
        tick = parse_global_quote("AAPL", bad_data)
        assert tick is None

    def test_empty_quote(self, empty_api_response: dict) -> None:
        """Returns None for an empty Global Quote."""
        tick = parse_global_quote("AAPL", empty_api_response)
        assert tick is None


# ---------------------------------------------------------------------------
# publish_tick
# ---------------------------------------------------------------------------
class TestPublishTick:
    """Tests for publishing ticks to Kafka."""

    def test_successful_publish(self) -> None:
        """Successfully publishes a tick to Kafka."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        tick = StockTick(
            symbol="AAPL",
            price=178.50,
            volume=52_000_000,
            timestamp=datetime(2024, 3, 15, 14, 30, tzinfo=timezone.utc),
        )

        result = publish_tick(mock_producer, tick)

        assert result is True
        mock_producer.send.assert_called_once_with(
            KAFKA_TOPIC,
            key="AAPL",
            value=tick.model_dump(mode="json"),
        )
        mock_future.get.assert_called_once_with(timeout=10)

    def test_kafka_error(self) -> None:
        """Returns False when Kafka raises an error."""
        from kafka.errors import KafkaError

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("Broker unavailable")
        mock_producer.send.return_value = mock_future

        tick = StockTick(
            symbol="AAPL",
            price=178.50,
            volume=52_000_000,
            timestamp=datetime(2024, 3, 15, 14, 30, tzinfo=timezone.utc),
        )

        result = publish_tick(mock_producer, tick)

        assert result is False


# ---------------------------------------------------------------------------
# backup_to_s3
# ---------------------------------------------------------------------------
class TestBackupToS3:
    """Tests for S3 bronze backup."""

    @patch("src.producers.stock_producer.upload_json_to_s3")
    @patch("src.producers.stock_producer.generate_s3_key")
    def test_successful_backup(
        self,
        mock_gen_key: MagicMock,
        mock_upload: MagicMock,
        valid_api_response: dict,
        test_settings: Settings,
    ) -> None:
        """Successfully backs up data to S3."""
        mock_gen_key.return_value = "bronze/stock_ticks/year=2024/AAPL.json"
        mock_upload.return_value = True

        backup_to_s3(valid_api_response, "AAPL", test_settings)

        mock_gen_key.assert_called_once_with("bronze", "stock_ticks", symbol="AAPL")
        mock_upload.assert_called_once()

    @patch("src.producers.stock_producer.upload_json_to_s3")
    @patch("src.producers.stock_producer.generate_s3_key")
    def test_s3_failure_non_fatal(
        self,
        mock_gen_key: MagicMock,
        mock_upload: MagicMock,
        valid_api_response: dict,
        test_settings: Settings,
    ) -> None:
        """S3 backup failure does not raise an exception."""
        mock_gen_key.return_value = "bronze/stock_ticks/test.json"
        mock_upload.side_effect = Exception("S3 error")

        # Should not raise
        backup_to_s3(valid_api_response, "AAPL", test_settings)


# ---------------------------------------------------------------------------
# run_producer
# ---------------------------------------------------------------------------
class TestRunProducer:
    """Tests for the main producer loop."""

    def test_kill_switch_disabled(self) -> None:
        """Returns 0 immediately when RUN_PIPELINE is False."""
        settings = Settings(
            alpha_vantage_api_key="test",
            run_pipeline=False,
            kafka_broker="localhost:9092",
        )
        result = run_producer(settings)
        assert result == 0

    def test_missing_api_key(self) -> None:
        """Returns 0 when API key is empty."""
        settings = Settings(
            alpha_vantage_api_key="",
            run_pipeline=True,
            kafka_broker="localhost:9092",
        )
        result = run_producer(settings)
        assert result == 0

    @patch("src.producers.stock_producer.backup_to_s3")
    @patch("src.producers.stock_producer.time.sleep")
    @patch("src.producers.stock_producer.publish_tick")
    @patch("src.producers.stock_producer.parse_global_quote")
    @patch("src.producers.stock_producer.fetch_global_quote")
    @patch("src.producers.stock_producer.create_kafka_producer")
    def test_single_iteration(
        self,
        mock_create_producer: MagicMock,
        mock_fetch: MagicMock,
        mock_parse: MagicMock,
        mock_publish: MagicMock,
        mock_sleep: MagicMock,
        mock_backup: MagicMock,
        test_settings: Settings,
    ) -> None:
        """Runs one iteration and publishes ticks for all symbols."""
        mock_producer = MagicMock()
        mock_create_producer.return_value = mock_producer

        mock_fetch.return_value = {"Global Quote": {"05. price": "100"}}

        tick = StockTick(
            symbol="TEST",
            price=100.0,
            volume=1000,
            timestamp=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        mock_parse.return_value = tick
        mock_publish.return_value = True

        result = run_producer(test_settings)

        assert result == 10  # One tick per symbol in watchlist
        assert mock_fetch.call_count == 10
        assert mock_publish.call_count == 10
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("src.producers.stock_producer.backup_to_s3")
    @patch("src.producers.stock_producer.time.sleep")
    @patch("src.producers.stock_producer.publish_tick")
    @patch("src.producers.stock_producer.parse_global_quote")
    @patch("src.producers.stock_producer.fetch_global_quote")
    @patch("src.producers.stock_producer.create_kafka_producer")
    def test_api_failure_skips_symbol(
        self,
        mock_create_producer: MagicMock,
        mock_fetch: MagicMock,
        mock_parse: MagicMock,
        mock_publish: MagicMock,
        mock_sleep: MagicMock,
        mock_backup: MagicMock,
        test_settings: Settings,
    ) -> None:
        """Skips symbols when API fetch fails."""
        mock_producer = MagicMock()
        mock_create_producer.return_value = mock_producer
        mock_fetch.return_value = None  # All fetches fail

        result = run_producer(test_settings)

        assert result == 0
        mock_publish.assert_not_called()

    @patch("src.producers.stock_producer.backup_to_s3")
    @patch("src.producers.stock_producer.time.sleep")
    @patch("src.producers.stock_producer.publish_tick")
    @patch("src.producers.stock_producer.parse_global_quote")
    @patch("src.producers.stock_producer.fetch_global_quote")
    @patch("src.producers.stock_producer.create_kafka_producer")
    def test_max_iterations_stops(
        self,
        mock_create_producer: MagicMock,
        mock_fetch: MagicMock,
        mock_parse: MagicMock,
        mock_publish: MagicMock,
        mock_sleep: MagicMock,
        mock_backup: MagicMock,
    ) -> None:
        """Stops after reaching max_iterations."""
        settings = Settings(
            alpha_vantage_api_key="test",
            run_pipeline=True,
            max_iterations=2,
            poll_interval_seconds=0,
            kafka_broker="localhost:9092",
        )
        mock_producer = MagicMock()
        mock_create_producer.return_value = mock_producer

        tick = StockTick(
            symbol="TEST",
            price=100.0,
            volume=1000,
            timestamp=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        mock_fetch.return_value = {"Global Quote": {"05. price": "100"}}
        mock_parse.return_value = tick
        mock_publish.return_value = True

        result = run_producer(settings)

        assert result == 20  # 10 symbols × 2 iterations

    @patch("src.producers.stock_producer.backup_to_s3")
    @patch("src.producers.stock_producer.time.sleep")
    @patch("src.producers.stock_producer.publish_tick")
    @patch("src.producers.stock_producer.parse_global_quote")
    @patch("src.producers.stock_producer.fetch_global_quote")
    @patch("src.producers.stock_producer.create_kafka_producer")
    def test_parse_failure_skips(
        self,
        mock_create_producer: MagicMock,
        mock_fetch: MagicMock,
        mock_parse: MagicMock,
        mock_publish: MagicMock,
        mock_sleep: MagicMock,
        mock_backup: MagicMock,
        test_settings: Settings,
    ) -> None:
        """Skips symbols when parsing fails."""
        mock_producer = MagicMock()
        mock_create_producer.return_value = mock_producer
        mock_fetch.return_value = {"Global Quote": {"05. price": "bad"}}
        mock_parse.return_value = None  # Parse fails

        result = run_producer(test_settings)

        assert result == 0
        mock_publish.assert_not_called()
