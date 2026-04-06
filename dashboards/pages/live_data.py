"""Live Data page — real-time price board, intraday chart, volume monitor."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboards.data_loader import (
    NAME_MAP,
    SECTOR_MAP,
    SYMBOLS,
    load_live_ticks,
)


def _compute_price_board(df: pd.DataFrame) -> pd.DataFrame:
    """Build a live price board from tick data.

    For each symbol, computes the latest price, the first price of the
    day, the price change, and the percentage change.

    Args:
        df: Tick-level DataFrame with symbol, price, volume, timestamp.

    Returns:
        One-row-per-symbol summary DataFrame.
    """
    rows = []
    for symbol in SYMBOLS:
        sym_df = df[df["symbol"] == symbol].sort_values("timestamp")
        if sym_df.empty:
            continue
        first_price = sym_df.iloc[0]["price"]
        latest = sym_df.iloc[-1]
        change = latest["price"] - first_price
        change_pct = (change / first_price) * 100 if first_price else 0
        total_vol = sym_df["volume"].sum()
        rows.append({
            "Symbol": symbol,
            "Company": NAME_MAP.get(symbol, symbol),
            "Sector": SECTOR_MAP.get(symbol, ""),
            "Latest Price": round(latest["price"], 2),
            "Change": round(change, 2),
            "Change %": round(change_pct, 2),
            "Volume": total_vol,
            "Last Updated": latest["timestamp"],
            "Ticks": len(sym_df),
        })
    return pd.DataFrame(rows)


def _highlight_change(val: float) -> str:
    """Apply green/red colour based on price change sign.

    Args:
        val: Numeric value to colour.

    Returns:
        CSS colour string.
    """
    if val > 0:
        return "color: green; font-weight: bold"
    if val < 0:
        return "color: red; font-weight: bold"
    return ""


def render() -> None:
    """Render the Live Data page."""
    st.header("Live Market Data")
    st.caption(
        "Real-time tick data from Kafka → Spark Streaming → S3 silver layer. "
        "Falls back to demo data when S3 is unavailable."
    )

    with st.spinner("Loading live ticks…"):
        ticks = load_live_ticks()

    if ticks.empty:
        st.warning("No live tick data available.")
        return

    board = _compute_price_board(ticks)
    if board.empty:
        st.warning("No symbols found in tick data.")
        return

    # --------------- Metrics row ---------------
    col1, col2, col3, col4 = st.columns(4)
    gainers = (board["Change %"] > 0).sum()
    losers = (board["Change %"] < 0).sum()
    latest_ts = ticks["timestamp"].max()

    col1.metric("Symbols Streaming", len(board))
    col2.metric("Gainers", int(gainers))
    col3.metric("Losers", int(losers))
    col4.metric("Last Tick", latest_ts.strftime("%H:%M:%S") if pd.notna(latest_ts) else "N/A")

    # --------------- Price board table ---------------
    st.subheader("Real-Time Price Board")

    styled = board.style.map(
        _highlight_change, subset=["Change", "Change %"]
    ).format({
        "Latest Price": "${:.2f}",
        "Change": "{:+.2f}",
        "Change %": "{:+.2f}%",
        "Volume": "{:,.0f}",
    })
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # --------------- Intraday price chart ---------------
    st.subheader("Intraday Price Movement")

    selected = st.multiselect(
        "Select symbols",
        SYMBOLS,
        default=SYMBOLS[:3],
        key="live_symbols",
    )

    if selected:
        filtered = ticks[ticks["symbol"].isin(selected)].sort_values("timestamp")
        fig = px.line(
            filtered,
            x="timestamp",
            y="price",
            color="symbol",
            labels={"price": "Price ($)", "timestamp": "Time", "symbol": "Symbol"},
            title="Intraday Price Ticks",
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    # --------------- Today's movers bar chart ---------------
    st.subheader("Today's Movers")

    movers = board.sort_values("Change %", ascending=False)
    fig_movers = go.Figure()
    fig_movers.add_trace(go.Bar(
        x=movers["Symbol"],
        y=movers["Change %"],
        marker_color=[
            "green" if v > 0 else "red" for v in movers["Change %"]
        ],
        text=[f"{v:+.2f}%" for v in movers["Change %"]],
        textposition="outside",
    ))
    fig_movers.update_layout(
        title="Price Change % (Today)",
        xaxis_title="Symbol",
        yaxis_title="Change %",
        showlegend=False,
    )
    st.plotly_chart(fig_movers, use_container_width=True)

    # --------------- Volume monitor ---------------
    st.subheader("Live Volume Monitor")

    vol_data = board[["Symbol", "Volume", "Ticks"]].sort_values(
        "Volume", ascending=False
    )
    fig_vol = px.bar(
        vol_data,
        x="Symbol",
        y="Volume",
        color="Volume",
        color_continuous_scale="Blues",
        labels={"Volume": "Total Volume"},
        title="Cumulative Volume (Today's Ticks)",
    )
    fig_vol.update_layout(showlegend=False)
    st.plotly_chart(fig_vol, use_container_width=True)

    # --------------- Pipeline health ---------------
    st.subheader("Pipeline Health")
    st.info(
        f"**Last tick received:** {latest_ts}  \n"
        f"**Total ticks loaded:** {len(ticks):,}  \n"
        f"**Symbols active:** {len(board)}/{len(SYMBOLS)}"
    )
