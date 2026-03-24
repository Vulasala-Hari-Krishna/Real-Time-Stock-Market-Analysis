"""Market Overview page — watchlist table, signals, sector bar chart."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.data_loader import (
    SECTOR_MAP,
    SYMBOLS,
    load_daily_summaries,
    load_sector_performance,
)


def _signal_color(signal: str) -> str:
    """Map a signal string to a CSS colour.

    Args:
        signal: Comma-separated signal string (e.g. 'OVERBOUGHT,VOLUME_SPIKE').

    Returns:
        CSS colour string.
    """
    if not signal:
        return "gray"
    s = signal.upper()
    if "OVERSOLD" in s or "GOLDEN_CROSS" in s:
        return "green"
    if "OVERBOUGHT" in s or "DEATH_CROSS" in s:
        return "red"
    if "VOLUME_SPIKE" in s:
        return "orange"
    return "gray"


def _build_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Build the latest-day summary row per symbol.

    Args:
        df: Daily summaries DataFrame.

    Returns:
        One-row-per-symbol DataFrame with key columns.
    """
    latest = df.sort_values("date").groupby("symbol").tail(1).copy()
    latest = latest[latest["symbol"].isin(SYMBOLS)]
    latest["sector"] = latest["symbol"].map(SECTOR_MAP)
    cols = [
        "symbol", "sector", "close", "daily_return_pct",
        "sma_20", "sma_50", "rsi_14", "volume", "signals",
    ]
    available_cols = [c for c in cols if c in latest.columns]
    return latest[available_cols].sort_values("symbol").reset_index(drop=True)


def render() -> None:
    """Render the Market Overview page."""
    st.header("Market Overview")

    with st.spinner("Loading daily summaries…"):
        summaries = load_daily_summaries()

    if summaries.empty:
        st.warning("No summary data available.")
        return

    table = _build_summary_table(summaries)

    # --------------- Metrics row ---------------
    col1, col2, col3, col4 = st.columns(4)
    avg_return = table["daily_return_pct"].mean() if "daily_return_pct" in table.columns else 0
    bullish = table["signals"].str.contains("OVERSOLD|GOLDEN_CROSS", na=False).sum() if "signals" in table.columns else 0
    bearish = table["signals"].str.contains("OVERBOUGHT|DEATH_CROSS", na=False).sum() if "signals" in table.columns else 0

    col1.metric("Stocks Tracked", len(table))
    col2.metric("Avg Daily Return", f"{avg_return:.2f}%")
    col3.metric("Bullish Signals", int(bullish))
    col4.metric("Bearish Signals", int(bearish))

    # --------------- Watchlist table ---------------
    st.subheader("Watchlist")

    def _highlight_signal(val: str) -> str:
        color = _signal_color(str(val))
        return f"color: {color}; font-weight: bold"

    styled = table.style
    if "signals" in table.columns:
        styled = styled.map(_highlight_signal, subset=["signals"])
    if "daily_return_pct" in table.columns:
        styled = styled.map(
            lambda v: "color: green" if v > 0 else ("color: red" if v < 0 else ""),
            subset=["daily_return_pct"],
        )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # --------------- Sector bar chart ---------------
    st.subheader("Sector Performance")

    with st.spinner("Loading sector data…"):
        sector_df = load_sector_performance()

    if sector_df.empty:
        st.info("No sector data available.")
        return

    latest_sector = sector_df.sort_values("date").groupby("sector").tail(1)
    fig = px.bar(
        latest_sector,
        x="sector",
        y="avg_return_pct",
        color="avg_return_pct",
        color_continuous_scale=["red", "gray", "green"],
        labels={"avg_return_pct": "Avg Return %", "sector": "Sector"},
        title="Latest Sector Avg Return %",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
