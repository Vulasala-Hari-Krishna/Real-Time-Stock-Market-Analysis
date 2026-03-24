"""Sector Analysis page — heatmaps, correlation matrix, gainers/losers."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboards.data_loader import (
    SECTOR_MAP,
    SYMBOLS,
    load_correlations,
    load_daily_summaries,
    load_sector_performance,
)


def _sector_heatmap(sector_df: pd.DataFrame) -> go.Figure:
    """Build a sector return heatmap over time.

    Args:
        sector_df: Sector performance DataFrame.

    Returns:
        Plotly Figure.
    """
    pivot = sector_df.pivot_table(
        index="sector", columns="date", values="avg_return_pct",
    )
    fig = px.imshow(
        pivot,
        color_continuous_scale="RdYlGn",
        labels={"color": "Avg Return %"},
        title="Sector Returns Heatmap",
        aspect="auto",
    )
    fig.update_layout(height=350)
    return fig


def _correlation_matrix(corr_df: pd.DataFrame) -> go.Figure:
    """Build a symmetric correlation matrix heatmap.

    Args:
        corr_df: Pairwise correlations DataFrame.

    Returns:
        Plotly Figure.
    """
    # Build a symmetric matrix
    matrix = pd.DataFrame(index=SYMBOLS, columns=SYMBOLS, dtype=float)
    for _, row in corr_df.iterrows():
        a, b = row["symbol_a"], row["symbol_b"]
        val = row["correlation"]
        if a in SYMBOLS and b in SYMBOLS:
            matrix.loc[a, b] = val
            matrix.loc[b, a] = val
    for s in SYMBOLS:
        matrix.loc[s, s] = 1.0

    fig = px.imshow(
        matrix.astype(float),
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        labels={"color": "Correlation"},
        title="Stock Correlation Matrix",
    )
    fig.update_layout(height=500)
    return fig


def _gainers_losers(summaries: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract the top 5 gainers and losers from the latest day.

    Args:
        summaries: Daily summaries DataFrame.

    Returns:
        Tuple of (gainers_df, losers_df).
    """
    latest = summaries.sort_values("date").groupby("symbol").tail(1).copy()
    latest["sector"] = latest["symbol"].map(SECTOR_MAP)
    latest = latest.sort_values("daily_return_pct", ascending=False)
    gainers = latest.head(5)[["symbol", "sector", "close", "daily_return_pct"]].reset_index(drop=True)
    losers = latest.tail(5)[["symbol", "sector", "close", "daily_return_pct"]].reset_index(drop=True)
    return gainers, losers


def render() -> None:
    """Render the Sector Analysis page."""
    st.header("Sector Analysis")

    with st.spinner("Loading data…"):
        sector_df = load_sector_performance()
        summaries = load_daily_summaries()
        corr_df = load_correlations()

    # --------------- Sector heatmap ---------------
    if not sector_df.empty:
        fig_heat = _sector_heatmap(sector_df)
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No sector performance data.")

    # --------------- Correlation matrix ---------------
    st.subheader("Correlation Matrix")
    if not corr_df.empty:
        fig_corr = _correlation_matrix(corr_df)
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("No correlation data available.")

    # --------------- Top gainers / losers ---------------
    st.subheader("Top Gainers & Losers")
    if not summaries.empty and "daily_return_pct" in summaries.columns:
        gainers, losers = _gainers_losers(summaries)
        g_col, l_col = st.columns(2)
        with g_col:
            st.markdown("**Top 5 Gainers**")
            st.dataframe(
                gainers.style.map(
                    lambda v: "color: green" if isinstance(v, (int, float)) and v > 0 else "",
                    subset=["daily_return_pct"],
                ),
                use_container_width=True,
                hide_index=True,
            )
        with l_col:
            st.markdown("**Top 5 Losers**")
            st.dataframe(
                losers.style.map(
                    lambda v: "color: red" if isinstance(v, (int, float)) and v < 0 else "",
                    subset=["daily_return_pct"],
                ),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No summary data for gainers/losers.")
