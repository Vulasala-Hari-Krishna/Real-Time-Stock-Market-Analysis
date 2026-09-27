"""Durable local cache for dashboard views backed by remote services (R7).

Bounded by construction: each dataset gets exactly one Parquet file plus one
JSON metadata sidecar, always overwritten in place - never a growing pile of
timestamped snapshots. This exists so a Snowflake (or similar) outage falls
back to the last real, known-good data with an honest "stale" banner, instead
of either silently re-querying a dead service on every page load or throwing
the last good data away in favor of synthetic demo data.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path(".state/dashboard-cache")


def _paths(dataset: str) -> tuple[Path, Path]:
    return CACHE_DIR / f"{dataset}.parquet", CACHE_DIR / f"{dataset}.meta.json"


def save_snapshot(dataset: str, df: pd.DataFrame, metadata: dict[str, Any]) -> None:
    """Persist the latest known-good snapshot for ``dataset``.

    Best-effort: a caller's successful remote load must never be broken by a
    local disk/permissions problem, so failures are logged, not raised.
    """
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        data_path, meta_path = _paths(dataset)
        tmp_data = data_path.with_suffix(".parquet.tmp")
        tmp_meta = meta_path.with_suffix(".meta.json.tmp")
        df.to_parquet(tmp_data)
        tmp_meta.write_text(json.dumps(metadata, default=str))
        # Replace both files only after both writes succeeded, so a crash
        # mid-write never leaves a data/metadata pair out of sync.
        os.replace(tmp_data, data_path)
        os.replace(tmp_meta, meta_path)
    except Exception:
        logger.exception("Failed to save local cache snapshot for %s", dataset)


def load_snapshot(dataset: str) -> Optional[tuple[pd.DataFrame, dict[str, Any]]]:
    """Load the last saved snapshot for ``dataset``.

    Returns:
        ``(df, metadata)``, or ``None`` when no cache exists yet or it is
        unreadable/corrupt - callers must treat that the same as "no cache".
    """
    data_path, meta_path = _paths(dataset)
    if not data_path.exists() or not meta_path.exists():
        return None
    try:
        df = pd.read_parquet(data_path)
        metadata = json.loads(meta_path.read_text())
        return df, metadata
    except Exception:
        logger.exception("Failed to load local cache snapshot for %s", dataset)
        return None
