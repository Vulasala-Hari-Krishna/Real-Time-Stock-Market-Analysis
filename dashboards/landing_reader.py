"""Live-tick reader: decodes raw Kafka envelopes straight from S3
``landing/ticks/`` - the same files ``src/consumers/raw_landing.py``
writes continuously (at least every ``raw_flush_interval_seconds``,
default 60s). This is the dashboard's "Live Data" source: genuinely
near-real-time, and independent of Databricks' own batched (every 15
minutes) promotion of the same files into Unity Catalog - Databricks owns
turning this raw capture into governed, aggregated tables, this module
just wants the last few minutes of it for a live view.

No local-cache/demo-data fallback here, unlike ``snowflake_loader.py``:
data turns over every ~60s, so there is nothing meaningful to cache for a
"live" view - a failed read just honestly reports "no live data available"
via ``LoadStatus(source="landing", ok=False, ...)``.
"""

import base64
import gzip
import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from dashboards.load_status import LoadStatus
from src.common.s3_utils import get_s3_client
from src.config.settings import get_settings

logger = logging.getLogger(__name__)

RECENT_MINUTES = 15


def _list_recent_keys(client, bucket: str, prefix: str, since: datetime) -> list[str]:
    """List every object under ``prefix`` last modified at or after ``since``."""
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"] >= since:
                keys.append(obj["Key"])
    return keys


def _decode_envelopes(body: bytes) -> list[dict]:
    """Decode one gzip NDJSON envelope file into the tick payloads it carries.

    Each line is one Kafka-record envelope (topic/partition/offset/
    kafka_timestamp/ingested_at/key_base64/value_base64/headers, per
    ``raw_landing.py::encode_record``) - the actual tick
    (symbol/price/volume/timestamp, per ``StockTick`` and exactly what
    ``stock_producer.py::publish_tick`` sends via
    ``tick.model_dump(mode="json")``) lives base64-encoded inside
    ``value_base64``. Malformed lines/envelopes are skipped, not fatal -
    one bad record must never blank out an otherwise-good file.
    """
    ticks = []
    for line in gzip.decompress(body).decode("utf-8").splitlines():
        if not line:
            continue
        try:
            envelope = json.loads(line)
            tick = json.loads(base64.b64decode(envelope["value_base64"]))
        except Exception:
            logger.debug("Skipping unparsable landing record", exc_info=True)
            continue
        ticks.append(tick)
    return ticks


def load_live_ticks(minutes: int = RECENT_MINUTES) -> tuple[pd.DataFrame, LoadStatus]:
    """Load the last ``minutes`` of ticks straight from S3 landing/ticks/.

    Returns:
        A DataFrame shaped like the legacy live-ticks view (``symbol``,
        ``price``, ``volume``, ``timestamp``), and an explicit LoadStatus.
        An empty DataFrame with ``ok=False`` means no live data is
        currently available (raw landing not configured, S3 unreachable,
        or genuinely nothing captured in the window) - never demo data.
    """
    settings = get_settings()
    if not settings.raw_source_id:
        return pd.DataFrame(
            columns=["symbol", "price", "volume", "timestamp"]
        ), LoadStatus(
            source="landing",
            ok=False,
            message="Raw landing is not configured (RAW_SOURCE_ID unset)",
        )

    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    prefix = (
        f"landing/ticks/source_id={settings.raw_source_id}/"
        f"topic={settings.raw_topic}/"
    )
    try:
        client = get_s3_client(settings.aws_default_region)
        keys = _list_recent_keys(client, settings.s3_bucket_name, prefix, since)
        rows: list[dict] = []
        for key in keys:
            body = client.get_object(Bucket=settings.s3_bucket_name, Key=key)[
                "Body"
            ].read()
            rows.extend(_decode_envelopes(body))
    except Exception as exc:
        logger.exception("Live landing read failed")
        return pd.DataFrame(
            columns=["symbol", "price", "volume", "timestamp"]
        ), LoadStatus(
            source="landing",
            ok=False,
            message=f"Live data unavailable: {exc}",
        )

    if not rows:
        return pd.DataFrame(
            columns=["symbol", "price", "volume", "timestamp"]
        ), LoadStatus(
            source="landing",
            ok=False,
            message=f"No ticks captured in the last {minutes} minutes",
        )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df, LoadStatus(
        source="landing",
        ok=True,
        message=f"Loaded {len(df)} ticks from the last {minutes} minutes",
        as_of=df["timestamp"].max(),
    )
