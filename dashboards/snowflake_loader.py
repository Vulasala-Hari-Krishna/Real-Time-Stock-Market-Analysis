"""Historical data loader for the Snowflake-backed dashboard view (R5).

Connects read-only, as the least-privilege ``reader`` role created by
``snowflake/terraform/workspace/main.tf`` - never the R4 loader role, never
ACCOUNTADMIN - and queries the published ``SERVING.DAILY_QUOTE_SUMMARY``
table. Unlike the rest of this dashboard's demo-data fallback (which is
silent beyond a static caption), this loader never lets a failed Snowflake
connection masquerade as a real result: every call returns an explicit
``LoadStatus`` alongside the DataFrame, and the page must render it as a
visible banner, not a quiet log line.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

from dashboards.data_loader import SYMBOLS

logger = logging.getLogger(__name__)

SERVING_TABLE = "DAILY_QUOTE_SUMMARY"

DASHBOARD_COLUMNS = [
    "provider",
    "symbol",
    "capture_date_utc",
    "first_observed_price",
    "highest_observed_price",
    "lowest_observed_price",
    "last_observed_price",
    "last_reported_volume",
    "quote_count",
    "first_quote_at",
    "last_quote_at",
    "observed_change_pct",
    "bronze_version",
    "loaded_batch_id",
    "loaded_at",
]


@dataclass
class LoadStatus:
    """Explicit outcome of one load - always shown to the viewer, never
    swallowed silently the way a bare demo-data fallback would be."""

    source: str  # "snowflake" | "demo"
    ok: bool
    message: str
    as_of: Optional[datetime] = None
    batch_id: Optional[str] = None


def _private_key_der(pem_text: str, passphrase: Optional[str]) -> bytes:
    from cryptography.hazmat.primitives import serialization

    # A local .env file (unlike a GitHub Actions secret) cannot reliably
    # hold a real multi-line value across every Docker Compose version, so
    # the documented local setup stores this as one line with literal
    # "\n" escapes. Un-escaping is always safe even when the value already
    # has real newlines (a valid PEM body never contains a literal
    # backslash - it's base64), so this handles both formats.
    pem_text = pem_text.replace("\\n", "\n")
    password = passphrase.encode("utf-8") if passphrase else None
    key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"), password=password
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    """Open a key-pair authenticated Snowflake connection as the read-only
    reader role. Raises if any required env var/credential is missing or
    the connection fails - callers must not treat that as demo-worthy,
    only as a clearly-flagged failure."""
    import snowflake.connector as sf

    pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    return sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_READER_ROLE", "STOCK_MARKET_DEV_READER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "STOCK_MARKET_DEV_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "STOCK_MARKET_DEV"),
        schema=os.environ.get("SNOWFLAKE_SERVING_SCHEMA", "SERVING"),
        private_key=_private_key_der(pem, passphrase),
    )


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_from_snowflake() -> pd.DataFrame:
    """Query SERVING.DAILY_QUOTE_SUMMARY. Cached for 5 minutes so repeated
    page renders/reruns don't re-open a warehouse session each time -
    "cached queries with least-privilege credentials" per the R5 contract.
    Streamlit's cache does not store a raised exception, so a transient
    failure is retried on the next call rather than getting stuck."""
    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM {SERVING_TABLE} ORDER BY symbol, capture_date_utc"
        )
        columns = [col[0].lower() for col in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        conn.close()


def _generate_demo_daily_quote_summary() -> pd.DataFrame:
    """Deterministic demo data shaped like SERVING.DAILY_QUOTE_SUMMARY.

    Only ever paired with an explicit LoadStatus(source="demo", ok=False) -
    never returned silently as if it were a real Snowflake result.
    """
    rng = np.random.default_rng(7)
    n_days = 60
    dates = pd.bdate_range(end=datetime.now(timezone.utc).date(), periods=n_days)
    base_prices = {
        "AAPL": 175.0, "MSFT": 420.0, "GOOGL": 155.0, "AMZN": 185.0,
        "TSLA": 250.0, "META": 500.0, "NVDA": 880.0, "JPM": 195.0,
        "V": 280.0, "JNJ": 155.0,
    }
    rows = []
    for symbol in SYMBOLS:
        price = base_prices.get(symbol, 100.0)
        for date in dates:
            open_price = price
            price *= 1 + rng.normal(0.0004, 0.012)
            high = max(open_price, price) * (1 + rng.uniform(0, 0.004))
            low = min(open_price, price) * (1 - rng.uniform(0, 0.004))
            rows.append({
                "provider": "demo",
                "symbol": symbol,
                "capture_date_utc": date.date(),
                "first_observed_price": round(open_price, 2),
                "highest_observed_price": round(high, 2),
                "lowest_observed_price": round(low, 2),
                "last_observed_price": round(price, 2),
                "last_reported_volume": int(rng.integers(200_000, 3_000_000)),
                "quote_count": int(rng.integers(20, 200)),
                "first_quote_at": pd.Timestamp(date, tz="UTC"),
                "last_quote_at": pd.Timestamp(date, tz="UTC") + pd.Timedelta(hours=8),
                "observed_change_pct": round((price / open_price - 1) * 100, 4),
                "bronze_version": None,
                "loaded_batch_id": "demo",
                "loaded_at": pd.Timestamp.now(tz="UTC"),
            })
    return pd.DataFrame(rows, columns=DASHBOARD_COLUMNS)


def load_daily_quote_summary() -> tuple[pd.DataFrame, LoadStatus]:
    """Load the Snowflake-served historical quote summary.

    Returns:
        The data, and an explicit LoadStatus the caller must render as a
        visible banner - on failure this is demo data clearly labelled
        source="demo", ok=False, never data that looks like a real result.
    """
    try:
        df = _fetch_from_snowflake()
        as_of = (
            pd.to_datetime(df["loaded_at"]).max()
            if "loaded_at" in df.columns and not df.empty
            else None
        )
        batch_id = (
            df["loaded_batch_id"].iloc[0]
            if "loaded_batch_id" in df.columns and not df.empty
            else None
        )
        return df, LoadStatus(
            source="snowflake",
            ok=True,
            message=f"Loaded {len(df)} rows from Snowflake",
            as_of=as_of,
            batch_id=batch_id,
        )
    except Exception as exc:
        logger.exception("Snowflake historical load failed")
        return _generate_demo_daily_quote_summary(), LoadStatus(
            source="demo",
            ok=False,
            message=f"Snowflake unavailable: {exc}",
        )
