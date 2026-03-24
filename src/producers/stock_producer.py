"""Kafka producer that ingests stock quotes from Alpha Vantage.

Polls the GLOBAL_QUOTE endpoint for each stock in the watchlist,
validates data via Pydantic, publishes to Kafka, and backs up raw
JSON to S3 bronze layer.
"""

import json
import logging
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common.s3_utils import generate_s3_key, upload_json_to_s3
from src.common.schemas import StockTick
from src.config.settings import Settings, get_settings
from src.config.watchlist import SYMBOLS

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
KAFKA_TOPIC = "raw_stock_ticks"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0
RATE_LIMIT_SLEEP_SECONDS = 12.0  # 5 calls/min on free tier


def fetch_global_quote(symbol: str, api_key: str) -> dict | None:
    """Fetch a single stock quote from Alpha Vantage GLOBAL_QUOTE endpoint.

    Args:
        symbol: Ticker symbol to query.
        api_key: Alpha Vantage API key.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": api_key,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                ALPHA_VANTAGE_BASE_URL,
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if "Global Quote" not in data or not data["Global Quote"]:
                logger.warning(
                    "Empty response for %s (attempt %d/%d): %s",
                    symbol,
                    attempt,
                    MAX_RETRIES,
                    data,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    continue
                return None

            return data

        except requests.exceptions.RequestException as exc:
            logger.error(
                "API request failed for %s (attempt %d/%d): %s",
                symbol,
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            else:
                return None

    return None


def parse_global_quote(symbol: str, data: dict) -> StockTick | None:
    """Parse Alpha Vantage GLOBAL_QUOTE response into a StockTick.

    Args:
        symbol: Ticker symbol.
        data: Raw API response dict.

    Returns:
        Validated StockTick, or None if parsing fails.
    """
    try:
        quote = data["Global Quote"]
        return StockTick(
            symbol=symbol,
            price=float(quote["05. price"]),
            volume=int(quote["06. volume"]),
            timestamp=datetime.now(tz=timezone.utc),
            source="alpha_vantage",
        )
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse quote for %s: %s", symbol, exc)
        return None


def create_kafka_producer(broker: str) -> KafkaProducer:
    """Create and return a KafkaProducer instance.

    Args:
        broker: Kafka broker connection string.

    Returns:
        Configured KafkaProducer.
    """
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publish_tick(producer: KafkaProducer, tick: StockTick) -> bool:
    """Publish a StockTick to the Kafka topic.

    Args:
        producer: KafkaProducer instance.
        tick: Validated StockTick to publish.

    Returns:
        True if published successfully, False otherwise.
    """
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=tick.symbol,
            value=tick.model_dump(mode="json"),
        )
        future.get(timeout=10)
        logger.info("Published tick: %s @ $%.2f", tick.symbol, tick.price)
        return True
    except KafkaError as exc:
        logger.error("Failed to publish tick for %s: %s", tick.symbol, exc)
        return False


def backup_to_s3(data: dict, symbol: str, settings: Settings) -> None:
    """Write raw API response to S3 bronze layer as backup.

    Args:
        data: Raw API response dict.
        symbol: Ticker symbol.
        settings: Application settings.
    """
    try:
        key = generate_s3_key("bronze", "stock_ticks", symbol=symbol)
        upload_json_to_s3(
            data=data,
            bucket=settings.s3_bucket_name,
            key=key,
            region=settings.aws_default_region,
        )
        logger.debug("Backed up raw data for %s to s3://%s/%s", symbol, settings.s3_bucket_name, key)
    except Exception as exc:
        logger.warning("S3 backup failed for %s (non-fatal): %s", symbol, exc)


def run_producer(settings: Settings | None = None) -> int:
    """Run the main producer loop.

    Polls Alpha Vantage for each stock in the watchlist, publishes
    ticks to Kafka, and backs up raw data to S3. Respects kill switch
    and iteration limits.

    Args:
        settings: Optional Settings override (useful for testing).

    Returns:
        Total number of ticks successfully published.
    """
    if settings is None:
        settings = get_settings()

    if not settings.run_pipeline:
        logger.info("Pipeline is disabled (RUN_PIPELINE=false). Exiting.")
        return 0

    if not settings.alpha_vantage_api_key:
        logger.error("ALPHA_VANTAGE_API_KEY is not set. Exiting.")
        return 0

    producer = create_kafka_producer(settings.kafka_broker)
    total_published = 0
    iteration = 0

    logger.info(
        "Starting producer: %d symbols, max_iterations=%d, poll_interval=%ds",
        len(SYMBOLS),
        settings.max_iterations,
        settings.poll_interval_seconds,
    )

    try:
        while True:
            iteration += 1
            logger.info("--- Iteration %d ---", iteration)

            for symbol in SYMBOLS:
                if not settings.run_pipeline:
                    logger.info("Kill switch activated. Stopping.")
                    return total_published

                data = fetch_global_quote(symbol, settings.alpha_vantage_api_key)
                if data is None:
                    continue

                tick = parse_global_quote(symbol, data)
                if tick is None:
                    continue

                if publish_tick(producer, tick):
                    total_published += 1

                backup_to_s3(data, symbol, settings)

                # Rate limit: Alpha Vantage free tier = 5 calls/min
                time.sleep(RATE_LIMIT_SLEEP_SECONDS)

            if settings.max_iterations > 0 and iteration >= settings.max_iterations:
                logger.info(
                    "Reached max iterations (%d). Stopping.", settings.max_iterations
                )
                break

            logger.info(
                "Iteration %d complete. Sleeping %ds before next poll.",
                iteration,
                settings.poll_interval_seconds,
            )
            time.sleep(settings.poll_interval_seconds)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer shut down. Total published: %d", total_published)

    return total_published


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_producer()
