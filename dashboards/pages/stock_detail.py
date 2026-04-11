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
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(count=2, label="2Y", step="year", stepmode="backward"),
                    dict(count=5, label="5Y", step="year", stepmode="backward"),
                    dict(label="All", step="all"),
                ],
            ),
            type="date",
        ),
    )
    return fig


def _fundamental_card(fund_row: pd.Series) -> None:
    """Render a fundamental data card.

    Args:
        fund_row: Single-row Series from the fundamentals table.
    """
    st.subheader("Fundamental Data")
    c1, c2, c3, c4 = st.columns(4)

    def _safe(val, default=None):
        """Return default if val is None, NaN, or missing."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return val

    mc = _safe(fund_row.get("market_cap"))
    if mc is None:
        mc_str = "N/A"
    elif mc >= 1e12:
        mc_str = f"${mc / 1e12:.2f}T"
    elif mc >= 1e9:
        mc_str = f"${mc / 1e9:.1f}B"
    else:
        mc_str = f"${mc / 1e6:.0f}M"
    c1.metric("Market Cap", mc_str)

    pe = _safe(fund_row.get("pe_ratio"))
    c2.metric("P/E Ratio", f"{pe:.1f}" if pe is not None else "N/A")
    fpe = _safe(fund_row.get("forward_pe"))
    c3.metric("Forward P/E", f"{fpe:.1f}" if fpe is not None else "N/A")
    div_yield = _safe(fund_row.get("dividend_yield"), 0)
    c4.metric("Dividend Yield", f"{div_yield * 100:.2f}%")

    c5, c6, c7, c8 = st.columns(4)
    eps = _safe(fund_row.get("eps"))
    c5.metric("EPS", f"${eps:.2f}" if eps is not None else "N/A")
    beta = _safe(fund_row.get("beta"))
    c6.metric("Beta", f"{beta:.2f}" if beta is not None else "N/A")
    high52 = _safe(fund_row.get("fifty_two_week_high"))
    c7.metric("52W High", f"${high52:.2f}" if high52 is not None else "N/A")
    low52 = _safe(fund_row.get("fifty_two_week_low"))
    c8.metric("52W Low", f"${low52:.2f}" if low52 is not None else "N/A")


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
    if signals:
        sig_list = [s.strip() for s in signals.split(",") if s.strip()]
        badges = " ".join(f"``{s}``" for s in sig_list)
        m4.markdown(f"**Signals**  \n{badges}")
    else:
        m4.metric("Signals", "None")

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
