"""Reusable validation and Spark transformations for landed fundamental
snapshots (R6, first migrated legacy product).

Unlike ticks, fundamentals land as a plain batch upload (no Kafka transport
identity to preserve) - each line is one symbol's snapshot from one fetcher
run, wrapped with an ``extraction_id`` for bronze lineage/replay tracking.
Canonical field validation reuses ``src.common.schemas.FundamentalData``,
the same model the legacy Spark job and its tests already use, so this
migration doesn't redefine the contract from scratch.
"""

import json
import re
from datetime import timezone
from typing import Any

from pydantic import ValidationError
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.schemas import FundamentalData

EXTRACTION_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"


def normalize_fundamentals_record(raw_json: str) -> dict[str, Any]:
    """Decode and validate one landed fundamentals line.

    Args:
        raw_json: Original NDJSON line retained in bronze - an
            ``extraction_id`` plus the ``FundamentalData`` fields.

    Returns:
        Typed fundamental fields or a non-sensitive rejection code.
    """
    result: dict[str, Any] = dict.fromkeys(
        [
            "extraction_id",
            "symbol",
            "retrieved_at",
            "market_cap",
            "pe_ratio",
            "forward_pe",
            "dividend_yield",
            "eps",
            "beta",
            "fifty_two_week_high",
            "fifty_two_week_low",
            "sector",
            "industry",
            "rejection_reason",
        ]
    )
    try:
        raw = json.loads(raw_json)
        if not isinstance(raw, dict):
            raise ValueError("not a JSON object")
        extraction_id = raw.pop("extraction_id")
    except (json.JSONDecodeError, ValueError, KeyError):
        result["rejection_reason"] = "invalid_record"
        return result
    if not isinstance(extraction_id, str) or not re.fullmatch(
        EXTRACTION_ID_PATTERN, extraction_id
    ):
        result["rejection_reason"] = "invalid_record"
        return result
    try:
        record = FundamentalData.model_validate(raw)
        if record.retrieved_at.utcoffset() is None:
            raise ValueError("retrieved_at must be timezone-aware")
    except (ValidationError, ValueError):
        result["rejection_reason"] = "invalid_fundamental"
        return result
    result.update(
        extraction_id=extraction_id,
        symbol=record.symbol,
        retrieved_at=record.retrieved_at.astimezone(timezone.utc).isoformat(),
        market_cap=record.market_cap,
        pe_ratio=record.pe_ratio,
        forward_pe=record.forward_pe,
        dividend_yield=record.dividend_yield,
        eps=record.eps,
        beta=record.beta,
        fifty_two_week_high=record.fifty_two_week_high,
        fifty_two_week_low=record.fifty_two_week_low,
        sector=record.sector,
        industry=record.industry,
    )
    return result


BUSINESS_KEYS = ["symbol"]
NORMALIZED_SCHEMA = (
    "extraction_id string, symbol string, retrieved_at string, "
    "market_cap double, pe_ratio double, forward_pe double, "
    "dividend_yield double, eps double, beta double, "
    "fifty_two_week_high double, fifty_two_week_low double, "
    "sector string, industry string, rejection_reason string"
)


def classify_fundamentals(bronze: DataFrame) -> DataFrame:
    """Classify every bronze line as accepted, superseded, or quarantined.

    Unlike ticks (no transport layer here - a landed file is a plain batch
    upload, not a replayable Kafka message), every symbol is expected to be
    re-fetched periodically. The latest valid snapshot per symbol is
    "accepted"; older valid snapshots for the same symbol are "superseded"
    (an expected refresh, not a data-quality conflict) rather than dropped
    silently - bronze retains them regardless since it is append-only.

    Args:
        bronze: Raw lines and file lineage from a pinned bronze Delta version.

    Returns:
        One classified row per input line, retaining the original raw JSON.
    """
    parse = F.udf(normalize_fundamentals_record, NORMALIZED_SCHEMA)
    parsed = (
        bronze.withColumn("normalized", parse("raw_json"))
        .select("*", "normalized.*")
        .drop("normalized")
        .withColumn("raw_sha256", F.sha2("raw_json", 256))
        .withColumn("retrieved_at", F.to_timestamp("retrieved_at"))
    )
    order = Window.partitionBy(*BUSINESS_KEYS).orderBy(
        F.when(F.col("rejection_reason").isNull(), 0).otherwise(1),
        F.col("retrieved_at").desc(),
        "file_path",
        "raw_sha256",
    )
    return (
        parsed.withColumn("business_rank", F.row_number().over(order))
        .withColumn(
            "record_status",
            F.when(F.col("rejection_reason").isNotNull(), "quarantined")
            .when(F.col("business_rank") > 1, "superseded")
            .otherwise("accepted"),
        )
        .drop("business_rank")
    )


def project_fundamentals(accepted: DataFrame) -> DataFrame:
    """Drop bronze-only bookkeeping columns, leaving the gold-ready projection.

    Args:
        accepted: Latest valid snapshot per symbol (record_status="accepted").

    Returns:
        One row per symbol with only the published fundamental fields.
    """
    return accepted.select(
        "symbol",
        "retrieved_at",
        "market_cap",
        "pe_ratio",
        "forward_pe",
        "dividend_yield",
        "eps",
        "beta",
        "fifty_two_week_high",
        "fifty_two_week_low",
        "sector",
        "industry",
    )
