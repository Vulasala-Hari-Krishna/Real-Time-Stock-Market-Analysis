"""Reusable validation and Spark transformations for landed quote samples."""

import base64
import binascii
import hashlib
import json
import math
import re
from datetime import timezone
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.schemas import StockTick


class _Header(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    key: str
    value_base64: str | None


class _Envelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    schema_version: int = Field(ge=1, le=1)
    source_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
    topic: str = Field(pattern=r"^[A-Za-z0-9_-][A-Za-z0-9_.-]*$")
    partition: int = Field(ge=0, le=2147483647)
    offset: int = Field(ge=0, le=9223372036854775807)
    kafka_timestamp: AwareDatetime | None
    ingested_at: AwareDatetime
    key_base64: str | None
    value_base64: str | None
    headers: list[_Header]


def normalize_envelope(raw_json: str) -> dict[str, Any]:
    """Decode a version-one envelope, retaining identity for replay checks.

    Args:
        raw_json: Original NDJSON line retained in bronze.

    Returns:
        Typed quote fields or a non-sensitive rejection code. Quote timestamps
        are producer capture times, not asserted exchange trade times.
    """
    result: dict[str, Any] = dict.fromkeys(
        [
            "source_id",
            "topic",
            "partition",
            "offset",
            "ingested_at",
            "payload_sha256",
            "symbol",
            "price",
            "reported_volume",
            "quote_timestamp",
            "provider",
            "rejection_reason",
        ]
    )
    try:
        envelope = _Envelope.model_validate_json(raw_json)
    except ValidationError:
        result["rejection_reason"] = "invalid_envelope"
        return result
    result.update(
        source_id=envelope.source_id,
        topic=envelope.topic,
        partition=envelope.partition,
        offset=envelope.offset,
        ingested_at=envelope.ingested_at.astimezone(timezone.utc).isoformat(),
        payload_sha256=hashlib.sha256(
            json.dumps(
                envelope.model_dump(mode="json", exclude={"ingested_at"}),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    )
    try:
        for encoded in [envelope.key_base64, envelope.value_base64] + [
            header.value_base64 for header in envelope.headers
        ]:
            if encoded is not None:
                base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        result["rejection_reason"] = "invalid_base64"
        return result
    if envelope.value_base64 is None:
        result["rejection_reason"] = "tombstone"
        return result
    try:
        payload = base64.b64decode(envelope.value_base64, validate=True).decode("utf-8")
        tick = StockTick.model_validate_json(payload, strict=True)
        if (
            not math.isfinite(tick.price)
            or tick.volume > 9223372036854775807
            or tick.timestamp.utcoffset() is None
            or not re.fullmatch(r"[A-Z0-9][A-Z0-9.^-]{0,9}", tick.symbol)
            or not tick.source.strip()
        ):
            raise ValueError("Invalid canonical quote fields")
    except (ValidationError, ValueError, UnicodeError):
        result["rejection_reason"] = "invalid_tick"
        return result
    result.update(
        symbol=tick.symbol,
        price=tick.price,
        reported_volume=tick.volume,
        quote_timestamp=tick.timestamp.astimezone(timezone.utc).isoformat(),
        provider=tick.source,
    )
    return result


TRANSPORT_KEYS = ["source_id", "topic", "partition", "offset"]
QUOTE_KEYS = ["provider", "symbol", "quote_timestamp"]
NORMALIZED_SCHEMA = (
    "source_id string, topic string, partition int, offset long, "
    "ingested_at string, payload_sha256 string, symbol string, price double, "
    "reported_volume long, quote_timestamp string, provider string, rejection_reason string"
)


def classify_ticks(bronze: DataFrame) -> DataFrame:
    """Classify every bronze line as accepted, duplicate, or quarantined.

    Conflicting content for one transport/business key is quarantined, not
    resolved by arbitrary row order. Exact replays choose a stable representative.

    Args:
        bronze: Raw lines and file lineage from a pinned bronze Delta version.

    Returns:
        One classified row per input line, retaining the original raw JSON.
    """
    parse = F.udf(normalize_envelope, NORMALIZED_SCHEMA)
    parsed = (
        bronze.withColumn("normalized", parse("raw_json"))
        .select("*", "normalized.*")
        .drop("normalized")
        .withColumn("raw_sha256", F.sha2("raw_json", 256))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("quote_timestamp", F.to_timestamp("quote_timestamp"))
    )
    transport_counts = parsed.groupBy(*TRANSPORT_KEYS).agg(
        F.countDistinct("payload_sha256").alias("transport_variants")
    )
    transport_order = Window.partitionBy(*TRANSPORT_KEYS).orderBy(
        "ingested_at", "file_path", "raw_sha256"
    )
    ranked = (
        parsed.join(transport_counts, TRANSPORT_KEYS, "left")
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("transport_variants") > 1, "transport_identity_conflict"
            ).otherwise(F.col("rejection_reason")),
        )
        .withColumn("transport_rank", F.row_number().over(transport_order))
    )
    candidates = ranked.filter(
        F.col("rejection_reason").isNull() & (F.col("transport_rank") == 1)
    )
    business_counts = candidates.groupBy(*QUOTE_KEYS).agg(
        F.countDistinct(F.struct("price", "reported_volume")).alias("quote_variants")
    )
    return (
        ranked.join(business_counts, QUOTE_KEYS, "left")
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("rejection_reason").isNull() & (F.col("quote_variants") > 1),
                "business_key_conflict",
            ).otherwise(F.col("rejection_reason")),
        )
        .withColumn(
            "quote_rank",
            F.row_number().over(
                Window.partitionBy(*QUOTE_KEYS).orderBy(
                    F.when(F.col("rejection_reason").isNull(), 0).otherwise(1),
                    "transport_rank",
                    *TRANSPORT_KEYS,
                    "file_path",
                    "raw_sha256",
                )
            ),
        )
        .withColumn(
            "record_status",
            F.when(F.col("rejection_reason").isNotNull(), "quarantined")
            .when(
                (F.col("transport_rank") > 1) | (F.col("quote_rank") > 1), "duplicate"
            )
            .otherwise("accepted"),
        )
        .drop("transport_variants", "quote_variants", "transport_rank", "quote_rank")
    )


def summarize_quotes(silver: DataFrame) -> DataFrame:
    """Summarize sampled quotes by UTC capture day, not exchange trading session.

    Args:
        silver: Unique accepted quote samples.

    Returns:
        Daily observed prices and the latest reported volume, never summed volume.
    """
    order = F.struct("quote_timestamp", *TRANSPORT_KEYS)
    return (
        silver.withColumn("capture_date_utc", F.to_date("quote_timestamp"))
        .groupBy("provider", "symbol", "capture_date_utc")
        .agg(
            F.min_by("price", order).alias("first_observed_price"),
            F.max("price").alias("highest_observed_price"),
            F.min("price").alias("lowest_observed_price"),
            F.max_by("price", order).alias("last_observed_price"),
            F.max_by("reported_volume", order).alias("last_reported_volume"),
            F.count("*").alias("quote_count"),
            F.min("quote_timestamp").alias("first_quote_at"),
            F.max("quote_timestamp").alias("last_quote_at"),
        )
        .withColumn(
            "observed_change_pct",
            (F.col("last_observed_price") / F.col("first_observed_price") - 1) * 100,
        )
    )
