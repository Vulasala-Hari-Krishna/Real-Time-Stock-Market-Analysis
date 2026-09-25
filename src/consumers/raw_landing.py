"""Opt-in lossless Kafka landing, separate from the legacy silver consumer."""

import base64
import gzip
import hashlib
import json
import logging
import signal
import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata

from src.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


def encode_record(record: Any, source_id: str, captured_at: datetime) -> dict[str, Any]:
    """Encode a Kafka record without parsing or filtering the original payload.

    Args:
        record: Kafka ConsumerRecord including original bytes and headers.
        source_id: Stable identifier for this Kafka source incarnation.
        captured_at: Timezone-aware local capture time.

    Returns:
        A JSON-compatible version-one raw envelope.

    Raises:
        ValueError: When capture time or transport identity is invalid.
    """
    if captured_at.utcoffset() is None:
        raise ValueError("Capture time must include a timezone")
    if not source_id or record.partition < 0 or record.offset < 0:
        raise ValueError("Source ID and nonnegative Kafka identity are required")

    def encode_bytes(value: bytes | None) -> str | None:
        return None if value is None else base64.b64encode(value).decode("ascii")

    kafka_timestamp = (
        datetime.fromtimestamp(record.timestamp / 1000, tz=timezone.utc).isoformat()
        if record.timestamp is not None and record.timestamp >= 0
        else None
    )
    return {
        "schema_version": 1,
        "source_id": source_id,
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "kafka_timestamp": kafka_timestamp,
        "ingested_at": captured_at.astimezone(timezone.utc).isoformat(),
        "key_base64": encode_bytes(record.key),
        "value_base64": encode_bytes(record.value),
        "headers": [
            {"key": key, "value_base64": encode_bytes(value)}
            for key, value in (record.headers or [])
        ],
    }


@contextmanager
def open_spool(settings: Settings) -> Iterator[sqlite3.Connection]:
    """Open an exclusively locked spool bound to one source and destination.

    Args:
        settings: Raw consumer configuration.

    Yields:
        Durable SQLite connection, closed on exit.

    Raises:
        ValueError: If a different source/destination tries to reuse this spool.
        sqlite3.OperationalError: If another process owns the spool.
    """
    settings.raw_spool_path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(settings.raw_spool_path, timeout=0)
    try:
        database.execute("PRAGMA locking_mode=EXCLUSIVE")
        database.execute("PRAGMA synchronous=FULL")
        with database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS identity (binding TEXT NOT NULL)"
            )
            database.execute(
                "CREATE TABLE IF NOT EXISTS records ("
                "topic TEXT NOT NULL, partition INTEGER NOT NULL, "
                "offset INTEGER NOT NULL, envelope TEXT NOT NULL, "
                "PRIMARY KEY (topic, partition, offset))"
            )
            binding = json.dumps(
                {
                    "version": 1,
                    "source": settings.raw_source_id,
                    "broker": settings.kafka_broker,
                    "group": settings.raw_consumer_group,
                    "topic": settings.raw_topic,
                    "bucket": settings.s3_bucket_name,
                    "region": settings.aws_default_region,
                },
                sort_keys=True,
            )
            existing = database.execute("SELECT binding FROM identity").fetchone()
            if existing is None:
                database.execute("INSERT INTO identity VALUES (?)", (binding,))
            elif existing[0] != binding:
                raise ValueError("Spool identity changed; use a separate spool path")
        yield database
    finally:
        database.close()


def stage_records(
    database: sqlite3.Connection, records: Iterable[Any], settings: Settings
) -> None:
    """Persist a poll atomically before attempting S3 I/O.

    Args:
        database: Open spool.
        records: Unparsed Kafka records.
        settings: Source and batch-size configuration.

    Raises:
        ValueError: When a record exceeds the configured envelope size limit.
    """
    captured_at = datetime.now(tz=timezone.utc)
    with database:
        for record in records:
            envelope = encode_record(record, settings.raw_source_id, captured_at)
            encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
            if len(encoded.encode("utf-8")) + 1 > settings.raw_max_batch_bytes:
                raise ValueError(
                    "Raw envelope exceeds RAW_MAX_BATCH_BYTES; not committed"
                )
            database.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?)",
                (record.topic, record.partition, record.offset, encoded),
            )


def upload_batch(client: Any, bucket: str, key: str, body: bytes) -> None:
    """Upload a content-addressed batch, verifying any existing object on retry.

    Args:
        client: S3 client with bounded request timeouts/retries.
        bucket: Configured landing bucket.
        key: Content-addressed landing object key.
        body: Gzip NDJSON bytes.

    Raises:
        ValueError: If an existing object's bytes conflict with this batch.
        ClientError: If reading or writing S3 fails.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
            raise
    else:
        with response["Body"] as stream:
            existing = stream.read(len(body) + 1)
        if existing != body:
            raise ValueError(
                "Existing landing object differs from content-addressed batch"
            )
        return
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
        ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"),
    )


def flush_spool(
    database: sqlite3.Connection,
    client: Any,
    settings: Settings,
    consumer: Any = None,
) -> int:
    """Upload bounded partition batches, then commit offsets and remove spool rows.

    Recovery omits the consumer: never commit old offsets across a rebalance.
    Broker replay may duplicate uploaded records; their transport keys are stable.

    Args:
        database: Open spool containing captured records.
        client: S3 client.
        settings: Landing configuration.
        consumer: Active consumer, or None for startup recovery.

    Returns:
        Number of records successfully uploaded and removed from the spool.
    """
    written = 0
    while rows := database.execute(
        "SELECT topic, partition, offset, envelope FROM records "
        "ORDER BY topic, partition, offset LIMIT ?",
        (settings.raw_max_records,),
    ).fetchall():
        topic, partition = rows[0][:2]
        lines: list[bytes] = []
        size = 0
        last_offset = rows[0][2]
        for row_topic, row_partition, offset, encoded in rows:
            line = (encoded + "\n").encode("utf-8")
            if len(line) > settings.raw_max_batch_bytes:
                raise ValueError("Pending envelope exceeds RAW_MAX_BATCH_BYTES")
            if lines and (
                (row_topic, row_partition) != (topic, partition)
                or size + len(line) > settings.raw_max_batch_bytes
            ):
                break
            lines.append(line)
            size += len(line)
            last_offset = offset
        body = gzip.compress(b"".join(lines), mtime=0)
        digest = hashlib.sha256(body).hexdigest()
        ingestion_date = json.loads(rows[0][3])["ingested_at"][:10]
        key = (
            f"landing/ticks/source_id={settings.raw_source_id}/topic={topic}/"
            f"partition={partition}/ingestion_date={ingestion_date}/"
            f"{rows[0][2]}-{last_offset}-{digest}.json.gz"
        )
        upload_batch(client, settings.s3_bucket_name, key, body)
        if consumer is not None:
            metadata = OffsetAndMetadata(
                last_offset + 1,
                "",
                **(
                    {"leader_epoch": -1}
                    if "leader_epoch" in OffsetAndMetadata._fields
                    else {}
                ),
            )
            consumer.commit({TopicPartition(topic, partition): metadata})
        with database:
            database.execute(
                "DELETE FROM records WHERE topic=? AND partition=? AND offset<=?",
                (topic, partition, last_offset),
            )
        written += len(lines)
        logger.info(
            "Landed topic=%s partition=%s through offset=%s",
            topic,
            partition,
            last_offset,
        )
    return written


class _RawAssignment(ConsumerRebalanceListener):
    def __init__(
        self, consumer: Any, recover_pending: Callable[[], None] = lambda: None
    ) -> None:
        self.consumer = consumer
        self.recover_pending = recover_pending
        self.failure: Exception | None = None

    def on_partitions_revoked(self, partitions: list[Any]) -> None:
        try:
            self.recover_pending()
        except Exception as error:
            self.failure = error
            raise

    def on_partitions_assigned(self, partitions: list[Any]) -> None:
        try:
            self._assign(partitions)
        except Exception as error:
            self.failure = error
            raise

    def _assign(self, partitions: list[Any]) -> None:
        beginnings = self.consumer.beginning_offsets(partitions)
        ends = self.consumer.end_offsets(partitions)
        for partition in partitions:
            committed = self.consumer.committed(partition)
            if committed is None:
                self.consumer.seek(partition, beginnings[partition])
            elif beginnings[partition] <= committed <= ends[partition]:
                self.consumer.seek(partition, committed)
            else:
                raise RuntimeError(
                    "Committed Kafka offset is unavailable; recovery required"
                )


def run_raw_consumer(
    settings: Settings, should_stop: Callable[[], bool] = lambda: False
) -> int:
    """Run opt-in raw capture without invoking Spark or cleansing market records.

    Args:
        settings: Validated local environment configuration.
        should_stop: Shutdown predicate checked between bounded polling requests.

    Returns:
        Number of landed records, including startup spool recovery.

    Raises:
        ValueError: When enabled without an explicit source incarnation ID.
    """
    if not settings.raw_landing_enabled:
        logger.info("Raw landing disabled; no clients started")
        return 0
    if not settings.raw_source_id:
        raise ValueError("Set RAW_SOURCE_ID before enabling raw landing")
    with open_spool(settings) as database:
        client = boto3.client(
            "s3",
            region_name=settings.aws_default_region,
            **(
                {
                    "aws_access_key_id": settings.aws_access_key_id,
                    "aws_secret_access_key": settings.aws_secret_access_key,
                    "aws_session_token": settings.aws_session_token or None,
                }
                if settings.aws_access_key_id
                else {}
            ),
            config=Config(
                connect_timeout=5,
                read_timeout=20,
                retries={"mode": "standard", "total_max_attempts": 3},
            ),
        )
        consumer = None
        try:
            written = flush_spool(database, client, settings)
            consumer = KafkaConsumer(
                bootstrap_servers=settings.kafka_broker,
                group_id=settings.raw_consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="none",
                max_poll_records=settings.raw_max_records,
                fetch_max_bytes=settings.raw_max_batch_bytes,
                max_partition_fetch_bytes=min(
                    settings.raw_max_batch_bytes, 1024 * 1024
                ),
                request_timeout_ms=30000,
                max_poll_interval_ms=300000,
                api_version_auto_timeout_ms=10000,
            )

            def recover_pending() -> None:
                nonlocal written
                written += flush_spool(database, client, settings)

            listener = _RawAssignment(consumer, recover_pending)
            consumer.subscribe([settings.raw_topic], listener=listener)
            last_flush = time.monotonic()
            while not should_stop():
                polled = consumer.poll(
                    timeout_ms=1000, max_records=settings.raw_max_records
                )
                if listener.failure is not None:
                    raise RuntimeError(
                        "Kafka rebalance failed; raw capture stopped"
                    ) from listener.failure
                stage_records(
                    database,
                    (record for records in polled.values() for record in records),
                    settings,
                )
                count, byte_count = database.execute(
                    "SELECT COUNT(*), COALESCE(SUM(LENGTH(envelope) + 1), 0) FROM records"
                ).fetchone()
                if count and (
                    count >= settings.raw_max_records
                    or byte_count >= settings.raw_max_batch_bytes
                    or time.monotonic() - last_flush
                    >= settings.raw_flush_interval_seconds
                ):
                    written += flush_spool(database, client, settings, consumer)
                    last_flush = time.monotonic()
            return written + flush_spool(database, client, settings, consumer)
        finally:
            try:
                if consumer is not None:
                    consumer.close(autocommit=False)
            finally:
                client.close()


def main() -> None:
    """Run raw landing until SIGINT/SIGTERM, flushing durable pending records."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    stopping = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stopping
        stopping = True

    previous = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        run_raw_consumer(get_settings(), lambda: stopping)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    main()
