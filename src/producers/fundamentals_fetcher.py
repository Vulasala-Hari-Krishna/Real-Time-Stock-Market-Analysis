"""Local fundamentals fetcher (R6): lands periodic yfinance snapshots to S3
``landing/fundamentals/`` for the Databricks bronze/silver/gold slice in
``src/batch/databricks_fundamentals.py`` to ingest via Auto Loader.

Unlike ``src/producers/stock_producer.py`` (a continuously-polling Kafka
producer), this is a standalone, manually/periodically run script: company
fundamentals change slowly, so there is no reason to poll continuously.
Each run fetches the whole watchlist and uploads one immutable,
content-addressed NDJSON batch - a full snapshot, not an incremental diff.
A failed/skipped symbol just means the next run's snapshot picks it up;
this never blocks on one bad yfinance response.
"""

import argparse
import base64
import gzip
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from botocore.exceptions import ClientError
from pydantic import ValidationError

from src.common.s3_utils import get_s3_client
from src.common.schemas import FundamentalData
from src.config.watchlist import SYMBOLS

logger = logging.getLogger(__name__)

# yfinance's unofficial Yahoo Finance API rate-limits bursts of requests
# (429 Too Many Requests) - confirmed live 2026-09-26 from a GitHub Actions
# runner: all 10 symbols were rejected within ~250ms of each other, the
# classic signature of a burst limit, not necessarily a permanent IP block.
# Retrying with backoff plus spacing requests apart is the standard
# mitigation. If 429s persist even with this, the runner's shared IP may
# be rate-limited/blocked by Yahoo more aggressively than a residential
# one, and this fetcher (a standalone script, not tied to GitHub Actions)
# should be run from a different network - e.g. locally - instead.
RETRY_DELAYS_SECONDS: tuple[float, ...] = (2.0, 5.0, 10.0)
SYMBOL_DELAY_SECONDS = 1.5

# yfinance Ticker.info key -> FundamentalData field name. Kept independent of
# src/batch/fundamental_enrichment.py's identical mapping so this fetcher
# never has to import that module's heavy PySpark dependency.
YFINANCE_FIELD_MAP: dict[str, str] = {
    "marketCap": "market_cap",
    "trailingPE": "pe_ratio",
    "forwardPE": "forward_pe",
    "dividendYield": "dividend_yield",
    "trailingEps": "eps",
    "beta": "beta",
    "fiftyTwoWeekHigh": "fifty_two_week_high",
    "fiftyTwoWeekLow": "fifty_two_week_low",
    "sector": "sector",
    "industry": "industry",
}


def fetch_one(
    symbol: str, sleep: Callable[[float], None] = time.sleep
) -> FundamentalData | None:
    """Fetch and validate one symbol's fundamentals; never raises.

    Retries transient yfinance/HTTP failures (e.g. 429 Too Many Requests)
    with backoff before giving up on this symbol.

    Args:
        symbol: Ticker symbol to fetch from yfinance.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        A validated record, or None if every attempt failed/validation
        failed - logged, not raised, so one bad symbol never blocks the
        rest of the run.
    """
    import yfinance as yf

    attempt_delays = (0.0, *RETRY_DELAYS_SECONDS)
    info: dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt, delay in enumerate(attempt_delays):
        if delay:
            sleep(delay)
        try:
            info = yf.Ticker(symbol).info
            last_error = None
            break
        except Exception as exc:  # yfinance raises a mix of exception types
            last_error = exc
            logger.warning(
                "yfinance fetch failed for %s (attempt %d/%d): %s",
                symbol,
                attempt + 1,
                len(attempt_delays),
                exc,
            )
    if last_error is not None or info is None:
        logger.error(
            "yfinance fetch permanently failed for %s after %d attempts",
            symbol,
            len(attempt_delays),
        )
        return None
    fields: dict[str, Any] = {
        target: info.get(source) for source, target in YFINANCE_FIELD_MAP.items()
    }
    try:
        return FundamentalData(
            symbol=symbol,
            retrieved_at=datetime.now(timezone.utc),
            **fields,
        )
    except ValidationError:
        logger.exception("Fundamentals validation failed for %s", symbol)
        return None


def build_batch(records: list[FundamentalData], extraction_id: str) -> bytes:
    """Serialize validated records as gzip NDJSON, each line self-describing.

    Args:
        records: Validated fundamentals, one per symbol.
        extraction_id: This run's identity, embedded in every line for
            bronze lineage - never inferred implicitly downstream.

    Returns:
        Gzip-compressed NDJSON bytes.
    """
    lines = []
    for record in records:
        payload = record.model_dump(mode="json")
        payload["extraction_id"] = extraction_id
        lines.append(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    body = ("\n".join(lines) + "\n").encode("utf-8")
    return gzip.compress(body, mtime=0)


def upload_batch(client: Any, bucket: str, key: str, body: bytes) -> None:
    """Upload a content-addressed batch, verifying any existing object on retry.

    Args:
        client: S3 client.
        bucket: Landing bucket.
        key: Content-addressed landing object key (unique per extraction_id).
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
                "Existing landing object differs from this content-addressed batch"
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


def run_fetch(
    bucket: str,
    symbols: list[str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """Fetch the whole watchlist and upload one immutable landing batch.

    Args:
        bucket: Target S3 bucket (existing project data lake bucket).
        symbols: Symbols to fetch; defaults to the full watchlist.
        sleep: Injectable delay function, so tests never actually block.

    Returns:
        Counts of fetched vs. skipped symbols.

    Raises:
        RuntimeError: If every symbol failed - never uploads an empty batch
            silently as if it were a legitimate zero-symbol snapshot.
    """
    symbols = symbols or SYMBOLS
    extraction_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    records = []
    for index, symbol in enumerate(symbols):
        if index:
            # Space requests apart to avoid tripping Yahoo's burst rate
            # limit in the first place, not just retrying after the fact.
            sleep(SYMBOL_DELAY_SECONDS)
        record = fetch_one(symbol, sleep=sleep)
        if record is not None:
            records.append(record)
    if not records:
        raise RuntimeError(
            f"All {len(symbols)} symbols failed to fetch; refusing to upload an empty batch"
        )
    body = build_batch(records, extraction_id)
    key = f"landing/fundamentals/extraction_id={extraction_id}/fundamentals.json.gz"
    client = get_s3_client()
    upload_batch(client, bucket, key, body)
    logger.info(
        "Uploaded %d/%d symbols to s3://%s/%s", len(records), len(symbols), bucket, key
    )
    return {"fetched": len(records), "skipped": len(symbols) - len(records)}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch yfinance fundamentals and land them to S3 for Databricks"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated override; defaults to the watchlist",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    symbols = args.symbols.split(",") if args.symbols else None
    run_fetch(args.bucket, symbols=symbols)


if __name__ == "__main__":
    main()
