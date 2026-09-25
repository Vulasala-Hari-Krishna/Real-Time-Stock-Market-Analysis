"""Hermetic tests for lossless raw Kafka landing and durable replay."""

import base64
import gzip
import hashlib
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from kafka import TopicPartition
from pydantic import ValidationError

from src.config.settings import Settings
from src.consumers.raw_landing import (
    _RawAssignment,
    encode_record,
    flush_spool,
    main,
    open_spool,
    run_raw_consumer,
    stage_records,
    upload_batch,
)


@pytest.fixture()
def raw_settings(tmp_path: Path) -> Settings:
    """Use isolated spool storage and no dotenv or service credentials."""
    return Settings(
        _env_file=None,
        raw_landing_enabled=True,
        raw_source_id="local-v1",
        raw_spool_path=tmp_path / "spool.sqlite3",
        s3_bucket_name="test-bucket",
    )


@pytest.fixture()
def record() -> SimpleNamespace:
    """Kafka-like record with a small malformed payload."""
    return SimpleNamespace(
        topic="raw_stock_ticks",
        partition=0,
        offset=0,
        timestamp=-1,
        key=None,
        value=b"bad-json",
        headers=[],
    )


@pytest.fixture()
def s3_client() -> MagicMock:
    """In-memory S3 double that preserves actual uploaded bytes."""
    client = MagicMock()
    objects: dict[str, bytes] = {}

    def get_object(**kwargs: object) -> dict:
        key = str(kwargs["Key"])
        if key not in objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(objects[key])}

    def put_object(**kwargs: object) -> None:
        objects[str(kwargs["Key"])] = kwargs["Body"]

    client.get_object.side_effect = get_object
    client.put_object.side_effect = put_object
    return client


def decoded_uploads(client: MagicMock) -> list[list[dict]]:
    """Decode complete NDJSON objects for behavior assertions."""
    return [
        [json.loads(line) for line in gzip.decompress(call.kwargs["Body"]).splitlines()]
        for call in client.put_object.call_args_list
    ]


def test_envelope_preserves_bytes_headers_and_transport_identity() -> None:
    """Malformed market data is retained rather than parsed or discarded."""
    captured_at = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
    record = SimpleNamespace(
        topic="raw_stock_ticks",
        partition=2,
        offset=17,
        timestamp=0,
        key=b"\xff",
        value=b"not-json\x00\xff",
        headers=[("trace", b"one"), ("trace", None), ("empty", b"")],
    )

    envelope = encode_record(record, "local-kafka-v1", captured_at)

    assert envelope == {
        "schema_version": 1,
        "source_id": "local-kafka-v1",
        "topic": "raw_stock_ticks",
        "partition": 2,
        "offset": 17,
        "kafka_timestamp": "1970-01-01T00:00:00+00:00",
        "ingested_at": "2026-09-23T12:00:00+00:00",
        "key_base64": base64.b64encode(b"\xff").decode("ascii"),
        "value_base64": base64.b64encode(b"not-json\x00\xff").decode("ascii"),
        "headers": [
            {"key": "trace", "value_base64": "b25l"},
            {"key": "trace", "value_base64": None},
            {"key": "empty", "value_base64": ""},
        ],
    }


@pytest.mark.parametrize("value", [None, b"", b"\x00\xff"])
def test_null_and_binary_payloads(record: SimpleNamespace, value: bytes | None) -> None:
    record.value = value
    record.headers = None
    envelope = encode_record(record, "source", datetime.now(timezone.utc))
    assert envelope["value_base64"] == (
        None if value is None else base64.b64encode(value).decode("ascii")
    )
    assert envelope["kafka_timestamp"] is None
    assert envelope["key_base64"] is None
    assert envelope["headers"] == []


def test_invalid_envelope_identity_and_time(record: SimpleNamespace) -> None:
    with pytest.raises(ValueError, match="timezone"):
        encode_record(record, "source", datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="Source ID"):
        encode_record(record, "", datetime.now(timezone.utc))
    record.offset = -1
    with pytest.raises(ValueError, match="nonnegative"):
        encode_record(record, "source", datetime.now(timezone.utc))


def test_upload_then_commit_and_checksum(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    consumer = MagicMock()
    events = MagicMock()
    events.attach_mock(s3_client.put_object, "upload")
    events.attach_mock(consumer.commit, "commit")
    with open_spool(raw_settings) as database:
        stage_records(database, [record], raw_settings)
        assert flush_spool(database, s3_client, raw_settings, consumer) == 1
        assert database.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 0
    assert [call[0] for call in events.mock_calls] == ["upload", "commit"]
    offset = consumer.commit.call_args.args[0][TopicPartition(record.topic, 0)]
    assert offset.offset == 1
    uploaded = s3_client.put_object.call_args.kwargs
    assert uploaded["Bucket"] == "test-bucket"
    assert uploaded["ContentEncoding"] == "gzip"
    digest = hashlib.sha256(uploaded["Body"]).digest()
    assert uploaded["ChecksumSHA256"] == base64.b64encode(digest).decode("ascii")
    assert uploaded["Key"].endswith(f"0-0-{digest.hex()}.json.gz")
    assert uploaded["Key"].startswith("landing/ticks/source_id=local-v1/")
    assert decoded_uploads(s3_client)[0][0]["value_base64"] == "YmFkLWpzb24="


def test_upload_failure_retains_spool_and_never_commits(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    consumer = MagicMock()
    s3_client.put_object.side_effect = OSError("upload interrupted")
    with open_spool(raw_settings) as database:
        stage_records(database, [record], raw_settings)
        with pytest.raises(OSError):
            flush_spool(database, s3_client, raw_settings, consumer)
    with open_spool(raw_settings) as database:
        assert database.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 1
    consumer.commit.assert_not_called()


def test_restart_after_upload_before_commit_reuses_identical_object(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    consumer = MagicMock()
    consumer.commit.side_effect = RuntimeError("rebalance")
    with open_spool(raw_settings) as database:
        stage_records(database, [record], raw_settings)
        with pytest.raises(RuntimeError, match="rebalance"):
            flush_spool(database, s3_client, raw_settings, consumer)
    original_key = s3_client.put_object.call_args.kwargs["Key"]
    with open_spool(raw_settings) as database:
        assert flush_spool(database, s3_client, raw_settings) == 1
        assert flush_spool(database, s3_client, raw_settings) == 0
    assert s3_client.put_object.call_count == 1
    assert s3_client.get_object.call_args.kwargs["Key"] == original_key
    assert consumer.commit.call_count == 1


def test_partial_multi_partition_failure_only_commits_uploaded_partition(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    other = SimpleNamespace(**{**vars(record), "partition": 1})
    consumer = MagicMock()
    original = s3_client.put_object.side_effect

    def fail_second(**kwargs: object) -> None:
        if "partition=1/" in str(kwargs["Key"]):
            raise OSError("partition one unavailable")
        original(**kwargs)

    s3_client.put_object.side_effect = fail_second
    with open_spool(raw_settings) as database:
        stage_records(database, [record, other], raw_settings)
        with pytest.raises(OSError):
            flush_spool(database, s3_client, raw_settings, consumer)
        assert database.execute("SELECT partition FROM records").fetchall() == [(1,)]
    assert list(consumer.commit.call_args.args[0]) == [TopicPartition(record.topic, 0)]


def test_batch_limits_partitioning_and_duplicate_staging(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    raw_settings.raw_max_records = 2
    records = [
        SimpleNamespace(**{**vars(record), "offset": offset}) for offset in range(5)
    ]
    with open_spool(raw_settings) as database:
        stage_records(database, records, raw_settings)
        before = database.execute(
            "SELECT envelope FROM records ORDER BY offset"
        ).fetchall()
        stage_records(database, records, raw_settings)
        assert (
            database.execute("SELECT envelope FROM records ORDER BY offset").fetchall()
            == before
        )
        assert flush_spool(database, s3_client, raw_settings) == 5
    assert [len(batch) for batch in decoded_uploads(s3_client)] == [2, 2, 1]
    assert [
        row["offset"] for batch in decoded_uploads(s3_client) for row in batch
    ] == list(range(5))


def test_byte_limits_and_oversized_record_atomic_rollback(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    raw_settings.raw_max_batch_bytes = 1024
    record.value = b"bytes" * 70
    other = SimpleNamespace(**{**vars(record), "offset": 1})
    with open_spool(raw_settings) as database:
        stage_records(database, [record, other], raw_settings)
        assert flush_spool(database, s3_client, raw_settings) == 2
        assert len(decoded_uploads(s3_client)) == 2
        oversized = SimpleNamespace(
            **{**vars(record), "offset": 2, "value": b"x" * 2048}
        )
        with pytest.raises(ValueError, match="RAW_MAX_BATCH_BYTES"):
            stage_records(database, [record, oversized], raw_settings)
        assert database.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 0


@pytest.mark.parametrize(
    "field", ["raw_source_id", "s3_bucket_name", "raw_consumer_group"]
)
def test_spool_rejects_identity_changes(raw_settings: Settings, field: str) -> None:
    with open_spool(raw_settings):
        pass
    setattr(raw_settings, field, "different")
    with pytest.raises(ValueError, match="Spool identity"):
        with open_spool(raw_settings):
            pass


def test_spool_rejects_simultaneous_owner(raw_settings: Settings) -> None:
    with open_spool(raw_settings):
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            with open_spool(raw_settings):
                pass


def test_recovery_rejects_pending_record_above_reduced_byte_limit(
    raw_settings: Settings, record: SimpleNamespace, s3_client: MagicMock
) -> None:
    record.value = b"bytes" * 400
    with open_spool(raw_settings) as database:
        stage_records(database, [record], raw_settings)
    raw_settings.raw_max_batch_bytes = 1024
    with open_spool(raw_settings) as database:
        with pytest.raises(ValueError, match="Pending envelope"):
            flush_spool(database, s3_client, raw_settings)
        assert database.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 1
    s3_client.put_object.assert_not_called()


@pytest.mark.parametrize("callback", ["revoke", "assign"])
def test_swallowed_rebalance_failure_stops_before_commit(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
    callback: str,
) -> None:
    consumer = MagicMock()
    partition = TopicPartition(record.topic, 0)
    calls = 0
    consumer.beginning_offsets.return_value = {partition: 10}
    consumer.end_offsets.return_value = {partition: 20}
    consumer.committed.return_value = 1
    s3_client.put_object.side_effect = OSError("upload failed")

    def poll(**kwargs: object) -> dict:
        nonlocal calls
        calls += 1
        if calls == 1:
            return {partition: [record]}
        listener = consumer.subscribe.call_args.kwargs["listener"]
        try:
            if callback == "revoke":
                listener.on_partitions_revoked([partition])
            else:
                listener.on_partitions_assigned([partition])
        except (OSError, RuntimeError):
            pass
        return {partition: [record]}

    consumer.poll.side_effect = poll
    with patch("src.consumers.raw_landing.boto3.client", return_value=s3_client), patch(
        "src.consumers.raw_landing.KafkaConsumer", return_value=consumer
    ):
        with pytest.raises(RuntimeError, match="Kafka rebalance failed"):
            run_raw_consumer(raw_settings)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with(autocommit=False)
    with open_spool(raw_settings) as database:
        assert database.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 1


def test_existing_object_conflict_is_not_overwritten(s3_client: MagicMock) -> None:
    upload_batch(s3_client, "bucket", "key", b"original")
    with pytest.raises(ValueError, match="differs"):
        upload_batch(s3_client, "bucket", "key", b"different")
    assert s3_client.put_object.call_count == 1


def test_s3_access_denied_does_not_attempt_write(s3_client: MagicMock) -> None:
    s3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "GetObject"
    )
    with pytest.raises(ClientError):
        upload_batch(s3_client, "bucket", "key", b"data")
    s3_client.put_object.assert_not_called()


@pytest.mark.parametrize("committed", [None, 10, 20])
def test_assignment_starts_at_committed_or_earliest_retained(
    committed: int | None,
) -> None:
    consumer = MagicMock()
    partition = TopicPartition("raw_stock_ticks", 0)
    consumer.beginning_offsets.return_value = {partition: 10}
    consumer.end_offsets.return_value = {partition: 20}
    consumer.committed.return_value = committed
    listener = _RawAssignment(consumer)
    listener.on_partitions_revoked([partition])
    listener.on_partitions_assigned([partition])
    consumer.seek.assert_called_once_with(
        partition, 10 if committed is None else committed
    )


@pytest.mark.parametrize("committed", [9, 21])
def test_assignment_fails_on_retention_gap_or_reset(committed: int) -> None:
    consumer = MagicMock()
    partition = TopicPartition("raw_stock_ticks", 0)
    consumer.beginning_offsets.return_value = {partition: 10}
    consumer.end_offsets.return_value = {partition: 20}
    consumer.committed.return_value = committed
    with pytest.raises(RuntimeError, match="recovery required"):
        _RawAssignment(consumer).on_partitions_assigned([partition])


@pytest.mark.parametrize("enabled,source", [(False, ""), (True, "")])
def test_opt_in_guard_precedes_all_io(
    raw_settings: Settings, enabled: bool, source: str
) -> None:
    raw_settings.raw_landing_enabled = enabled
    raw_settings.raw_source_id = source
    with patch("src.consumers.raw_landing.boto3.client") as client, patch(
        "src.consumers.raw_landing.KafkaConsumer"
    ) as consumer:
        if enabled:
            with pytest.raises(ValueError, match="RAW_SOURCE_ID"):
                run_raw_consumer(raw_settings)
        else:
            assert run_raw_consumer(raw_settings) == 0
        client.assert_not_called()
        consumer.assert_not_called()
    assert not raw_settings.raw_spool_path.exists()


@pytest.mark.parametrize("flush_on_size", [False, True])
def test_consumer_poll_flush_and_close(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
    flush_on_size: bool,
) -> None:
    if flush_on_size:
        raw_settings.raw_max_records = 1
    consumer = MagicMock()
    consumer.poll.side_effect = [{}, {TopicPartition(record.topic, 0): [record]}]
    stop = MagicMock(side_effect=[False, False, True])
    with patch("src.consumers.raw_landing.boto3.client", return_value=s3_client), patch(
        "src.consumers.raw_landing.KafkaConsumer", return_value=consumer
    ) as factory:
        assert run_raw_consumer(raw_settings, stop) == 1
    assert factory.call_args.kwargs["enable_auto_commit"] is False
    assert factory.call_args.kwargs["auto_offset_reset"] == "none"
    assert factory.call_args.kwargs["group_id"] == "stock-raw-landing-v1"
    consumer.close.assert_called_once_with(autocommit=False)
    s3_client.close.assert_called_once()
    consumer.commit.assert_called_once()


def test_consumer_timed_flush_and_poll_failure_cleanup(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    consumer = MagicMock()
    consumer.poll.side_effect = [
        {TopicPartition(record.topic, 0): [record]},
        RuntimeError("poll failed"),
    ]
    with patch("src.consumers.raw_landing.boto3.client", return_value=s3_client), patch(
        "src.consumers.raw_landing.KafkaConsumer", return_value=consumer
    ), patch("src.consumers.raw_landing.time.monotonic", side_effect=[0, 61, 61]):
        with pytest.raises(RuntimeError, match="poll failed"):
            run_raw_consumer(raw_settings)
    consumer.commit.assert_called_once()
    consumer.close.assert_called_once_with(autocommit=False)
    s3_client.close.assert_called_once()


def test_failed_startup_recovery_never_opens_kafka(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    with open_spool(raw_settings) as database:
        stage_records(database, [record], raw_settings)
    s3_client.put_object.side_effect = OSError("offline")
    with patch("src.consumers.raw_landing.boto3.client", return_value=s3_client), patch(
        "src.consumers.raw_landing.KafkaConsumer"
    ) as consumer:
        with pytest.raises(OSError):
            run_raw_consumer(raw_settings)
        consumer.assert_not_called()
    s3_client.close.assert_called_once()


def test_rebalance_flushes_pending_without_committing_old_assignment(
    raw_settings: Settings,
    record: SimpleNamespace,
    s3_client: MagicMock,
) -> None:
    consumer = MagicMock()
    calls = 0

    def poll(**kwargs: object) -> dict:
        nonlocal calls
        calls += 1
        if calls == 1:
            return {TopicPartition(record.topic, 0): [record]}
        consumer.subscribe.call_args.kwargs["listener"].on_partitions_revoked([])
        return {}

    consumer.poll.side_effect = poll
    stop = MagicMock(side_effect=[False, False, True])
    with patch("src.consumers.raw_landing.boto3.client", return_value=s3_client), patch(
        "src.consumers.raw_landing.KafkaConsumer", return_value=consumer
    ):
        assert run_raw_consumer(raw_settings, stop) == 1
    s3_client.put_object.assert_called_once()
    consumer.commit.assert_not_called()


def test_temporary_credentials_from_settings_are_passed_to_s3(
    raw_settings: Settings,
    s3_client: MagicMock,
) -> None:
    raw_settings.aws_access_key_id = "test-only-access"
    raw_settings.aws_secret_access_key = "test-only-secret"
    raw_settings.aws_session_token = "test-only-session"
    with patch(
        "src.consumers.raw_landing.boto3.client", return_value=s3_client
    ) as factory, patch("src.consumers.raw_landing.KafkaConsumer"):
        assert run_raw_consumer(raw_settings, lambda: True) == 0
    assert factory.call_args.kwargs["aws_session_token"] == "test-only-session"


@pytest.mark.parametrize(
    "values",
    [
        {"raw_max_records": 0},
        {"raw_flush_interval_seconds": 0},
        {"raw_max_batch_bytes": 1},
        {"raw_source_id": "../invalid"},
        {"raw_topic": "../invalid"},
    ],
)
def test_invalid_raw_configuration(values: dict) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


def test_raw_mode_defaults_off() -> None:
    assert Settings(_env_file=None).raw_landing_enabled is False


def test_entry_point_handles_stop_and_restores_signals(raw_settings: Settings) -> None:
    handlers: list = []

    def register(signum: int, handler: object) -> str:
        handlers.append(handler)
        return "previous"

    def run(settings: Settings, stopping: object) -> None:
        assert stopping() is False
        handlers[0](15, None)
        assert stopping() is True

    with patch(
        "src.consumers.raw_landing.get_settings", return_value=raw_settings
    ), patch(
        "src.consumers.raw_landing.signal.signal", side_effect=register
    ) as signals, patch(
        "src.consumers.raw_landing.run_raw_consumer", side_effect=run
    ):
        main()
    assert signals.call_count == 4
