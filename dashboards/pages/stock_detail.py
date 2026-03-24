"""Stock Detail page — individual stock deep-dive with interactive charts."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from dashboards.data_loader import (
    NAME_MAP,
    SYMBOLS,
    load_daily_summaries,
    load_fundamentals,
)


def _price_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Build a candlestick + SMA overlay chart.

    Args:
        df: Daily summaries for a single symbol, sorted by date.
        symbol: Ticker symbol.

    Returns:
        Plotly Figure.
    """
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.55, 0.25, 0.20],
        subplot_titles=(f"{symbol} Price", "RSI (14)", "Volume"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["date"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"], name="OHLC",
        ),
        row=1, col=1,
    )

    # SMA overlays
    for period, colour in [(20, "#1f77b4"), (50, "#ff7f0e"), (200, "#2ca02c")]:
        col_name = f"sma_{period}"
        if col_name in df.columns and df[col_name].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=df["date"], y=df[col_name],
                    mode="lines", name=f"SMA {period}",
                    line={"color": colour, "width": 1},
                ),
                row=1, col=1,
            )

    # RSI
    if "rsi_14" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["rsi_14"], name="RSI 14", line={"color": "purple"}),
            row=2, col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    # Volume with anomaly highlights
    colors = []
    if "volume_vs_avg" in df.columns:
        colors = [
            "red" if v > 2.0 else "steelblue"
            for v in df["volume_vs_avg"].fillna(1)
        ]
    else:
        colors = ["steelblue"] * len(df)

    fig.add_trace(
        go.Bar(x=df["date"], y=df["volume"], name="Volume", marker_color=colors),
        row=3, col=1,
    )

    fig.update_layout(
        height=750,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend={"orientation": "h", "y": 1.02, "x": 0.5, "xanchor": "center"},
    )
    return fig


def _fundamental_card(fund_row: pd.Series) -> None:
    """Render a fundamental data card.

    Args:
        fund_row: Single-row Series from the fundamentals table.
    """
    st.subheader("Fundamental Data")
    c1, c2, c3, c4 = st.columns(4)
    mc = fund_row.get("market_cap", 0)
    c1.metric("Market Cap", f"${mc / 1e9:.1f}B" if mc else "N/A")
    c2.metric("P/E Ratio", f"{fund_row.get('pe_ratio', 'N/A'):.1f}")
    c3.metric("Forward P/E", f"{fund_row.get('forward_pe', 'N/A'):.1f}")
    div_yield = fund_row.get("dividend_yield", 0) or 0
    c4.metric("Dividend Yield", f"{div_yield * 100:.2f}%")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("EPS", f"${fund_row.get('eps', 'N/A')}")
    c6.metric("Beta", f"{fund_row.get('beta', 'N/A'):.2f}")
    c7.metric("52W High", f"${fund_row.get('fifty_two_week_high', 'N/A')}")
    c8.metric("52W Low", f"${fund_row.get('fifty_two_week_low', 'N/A')}")


def render() -> None:
    """Render the Stock Detail page."""
    st.header("Stock Detail")

    symbol = st.selectbox(
        "Select Stock",
        SYMBOLS,
        format_func=lambda s: f"{s} — {NAME_MAP.get(s, s)}",
    )

    with st.spinner(f"Loading data for {symbol}…"):
        summaries = load_daily_summaries()

    stock_df = summaries[summaries["symbol"] == symbol].sort_values("date")

    if stock_df.empty:
        st.warning(f"No data for {symbol}.")
        return

    # Latest snapshot
    latest = stock_df.iloc[-1]
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Close", f"${latest['close']:.2f}")
    m2.metric("Daily Return", f"{latest.get('daily_return_pct', 0):.2f}%")
    m3.metric("RSI (14)", f"{latest.get('rsi_14', 0):.1f}")
    signals = latest.get("signals", "")
    m4.metric("Signals", signals if signals else "None")

    # Charts
    fig = _price_chart(stock_df, symbol)
    st.plotly_chart(fig, use_container_width=True)

    # Fundamentals
    with st.spinner("Loading fundamentals…"):
        fundamentals = load_fundamentals()

    fund = fundamentals[fundamentals["symbol"] == symbol]
    if not fund.empty:
        _fundamental_card(fund.iloc[0])
    else:
        st.info("No fundamental data available.")
