"""Snowflake History page — daily sampled-quote summaries loaded from
Databricks gold via the R4 snapshot loader into Snowflake
SERVING.DAILY_QUOTE_SUMMARY, read-only via the least-privilege reader
role (R5)."""

import plotly.express as px
import streamlit as st

from dashboards.data_loader import NAME_MAP, SECTOR_MAP
from dashboards.snowflake_loader import load_daily_quote_summary


def render() -> None:
    """Render the Snowflake History page."""
    st.header("Snowflake Historical Summary")
    st.caption(
        "Daily sampled-quote summaries: Databricks gold -> R4 snapshot "
        "loader -> Snowflake SERVING.DAILY_QUOTE_SUMMARY. Sampled quotes, "
        "not exchange OHLCV - distinct from the Market Overview page's "
        "legacy daily_summaries product."
    )

    with st.spinner("Querying Snowflake..."):
        df, status = load_daily_quote_summary()

    if status.source == "snowflake" and status.ok:
        as_of = (
            status.as_of.strftime("%Y-%m-%d %H:%M UTC")
            if status.as_of is not None
            else "unknown"
        )
        st.success(
            f"Live from Snowflake — {len(df)} rows, last loaded {as_of} "
            f"(batch `{status.batch_id}`)."
        )
    else:
        st.error(
            "⚠️ DEMO DATA — this is NOT real data. "
            f"Snowflake connection failed: {status.message}"
        )

    if df.empty:
        st.warning("No data available.")
        return

    symbols = sorted(df["symbol"].unique())
    selected = st.selectbox("Symbol", symbols)
    sym_df = df[df["symbol"] == selected].sort_values("capture_date_utc")

    col1, col2, col3 = st.columns(3)
    col1.metric("Company", NAME_MAP.get(selected, selected))
    col2.metric("Sector", SECTOR_MAP.get(selected, "—"))
    col3.metric("Days of history", len(sym_df))

    fig = px.line(
        sym_df,
        x="capture_date_utc",
        y="last_observed_price",
        title=f"{selected} — Last Observed Price by Day",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        sym_df[
            [
                "capture_date_utc",
                "first_observed_price",
                "highest_observed_price",
                "lowest_observed_price",
                "last_observed_price",
                "last_reported_volume",
                "quote_count",
                "observed_change_pct",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
