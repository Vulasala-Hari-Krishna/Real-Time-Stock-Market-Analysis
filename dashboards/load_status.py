"""Shared load-outcome type for every dashboard data loader.

Every loader that can fall back to cached or synthetic data (Snowflake
datasets, the live-tick landing reader) returns this alongside its
DataFrame, so a page can never silently show fake data as if it were real -
the caller must render it as a visible banner.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class LoadStatus:
    """Explicit outcome of one load - always shown to the viewer, never
    swallowed silently the way a bare demo-data fallback would be."""

    source: str  # "snowflake" | "cache" | "demo" | "landing"
    ok: bool
    message: str
    as_of: Optional[datetime] = None
    batch_id: Optional[str] = None


def render_status_banner(status: LoadStatus) -> None:
    """Render the shared honesty banner every dashboard page uses.

    Snowflake success and a live landing read both get a green banner;
    stale-but-real cached data gets an explicit yellow warning; anything
    else (demo data, or no live data currently available) gets a visible
    red/orange callout - never a silent fallback.
    """
    import streamlit as st

    if status.ok:
        as_of = (
            status.as_of.strftime("%Y-%m-%d %H:%M UTC")
            if status.as_of is not None
            else "unknown"
        )
        batch_suffix = f" (batch `{status.batch_id}`)" if status.batch_id else ""
        st.success(f"{status.message} — as of {as_of}{batch_suffix}.")
    elif status.source == "cache":
        st.warning(f"⚠️ USING CACHED DATA — real data, but stale. {status.message}")
    elif status.source == "demo":
        st.error(f"⚠️ DEMO DATA — this is NOT real data. {status.message}")
    else:
        st.warning(f"⚠️ {status.message}")
