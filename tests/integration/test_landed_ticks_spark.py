"""Opt-in, cloud-free Spark behavior checks: RUN_LOCAL_SPARK_TESTS=1."""

import base64
import json
import os

import pytest
from pyspark.sql import SparkSession

from src.batch.landed_ticks import classify_ticks, summarize_quotes

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_LOCAL_SPARK_TESTS") != "1",
    reason="Set RUN_LOCAL_SPARK_TESTS=1 for local Java/Spark execution",
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    """Run a small local Spark session without cloud connectors."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("landed-ticks-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    yield session
    session.stop()


def line(offset: int, price: float, hour: int, volume: int = 1000) -> str:
    """Build the same envelope shape as local capture."""
    return json.dumps(
        {
            "schema_version": 1,
            "source_id": "local-v1",
            "topic": "raw_stock_ticks",
            "partition": 0,
            "offset": offset,
            "kafka_timestamp": None,
            "ingested_at": "2026-09-23T20:00:00Z",
            "key_base64": None,
            "headers": [],
            "value_base64": base64.b64encode(
                json.dumps(
                    {
                        "symbol": "AAPL",
                        "price": price,
                        "volume": volume,
                        "timestamp": f"2026-09-23T{hour:02d}:00:00Z",
                        "source": "alpha_vantage",
                    }
                ).encode()
            ).decode(),
        }
    )


def test_replay_late_quotes_and_sampled_volume(spark: SparkSession) -> None:
    later = line(2, 110.0, 16, 1500)
    earlier = line(1, 100.0, 12, 1000)
    bronze = spark.createDataFrame(
        [
            (later, "second.gz"),
            (earlier, "first.gz"),
            (earlier, "replay.gz"),
            (line(3, 110.0, 16, 1500), "business-replay.gz"),
            ("bad-json", "bad.gz"),
        ],
        "raw_json string, file_path string",
    )
    classified = classify_ticks(bronze).cache()
    try:
        statuses = {
            row.record_status: row["count"]
            for row in classified.groupBy("record_status").count().collect()
        }
        assert statuses == {"accepted": 2, "duplicate": 2, "quarantined": 1}
        gold = summarize_quotes(classified.filter("record_status = 'accepted'")).first()
        assert gold.first_observed_price == 100.0
        assert gold.last_observed_price == 110.0
        assert gold.last_reported_volume == 1500
        assert gold.quote_count == 2
        assert gold.observed_change_pct == pytest.approx(10.0)
    finally:
        classified.unpersist()


def test_transport_and_business_conflicts_are_not_arbitrarily_resolved(
    spark: SparkSession,
) -> None:
    bronze = spark.createDataFrame(
        [
            (line(1, 100.0, 12), "one"),
            (line(1, 101.0, 12), "conflict"),
            (line(2, 110.0, 16), "two"),
            (line(3, 111.0, 16), "business-conflict"),
        ],
        "raw_json string, file_path string",
    )
    rows = classify_ticks(bronze).select("record_status", "rejection_reason").collect()
    assert all(row.record_status == "quarantined" for row in rows)
    assert sorted(row.rejection_reason for row in rows) == [
        "business_key_conflict",
        "business_key_conflict",
        "transport_identity_conflict",
        "transport_identity_conflict",
    ]


def test_quarantined_identity_does_not_hide_valid_quote(spark: SparkSession) -> None:
    bronze = spark.createDataFrame(
        [
            (line(1, 100.0, 12), "one"),
            (line(1, 101.0, 12), "conflict"),
            (line(2, 100.0, 12), "valid"),
        ],
        "raw_json string, file_path string",
    )
    accepted = classify_ticks(bronze).filter("record_status = 'accepted'").collect()
    assert len(accepted) == 1
    assert accepted[0].offset == 2


def test_empty_bronze_is_a_valid_empty_snapshot(spark: SparkSession) -> None:
    bronze = spark.createDataFrame([], "raw_json string, file_path string")
    accepted = classify_ticks(bronze).filter("record_status = 'accepted'")
    assert summarize_quotes(accepted).count() == 0
