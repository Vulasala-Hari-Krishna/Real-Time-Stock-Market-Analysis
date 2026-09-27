"""Real-Time Stock Market Analytics Dashboard.

Main Streamlit application entry point with sidebar navigation
to the overview, stock detail, and sector analysis pages.
"""

import streamlit as st

st.set_page_config(
    page_title="Stock Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("📈 Stock Analytics")
st.sidebar.markdown("Real-time market intelligence dashboard")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    [
        "Live Data",
        "Market Overview",
        "Stock Detail",
        "Sector Analysis",
        "Snowflake History",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Live Data reads straight from S3 landing/ticks/ (Kafka's raw capture)."
)
st.sidebar.caption(
    "Overview, Stock Detail, and Sector Analysis read from Snowflake (hybrid Databricks→Snowflake pipeline)."
)
st.sidebar.caption(
    "Snowflake History reads sampled-quote summaries from the same pipeline."
)

if page == "Live Data":
    from dashboards.pages.live_data import render

    render()
elif page == "Market Overview":
    from dashboards.pages.overview import render

    render()
elif page == "Stock Detail":
    from dashboards.pages.stock_detail import render

    render()
elif page == "Sector Analysis":
    from dashboards.pages.sector_analysis import render

    render()
elif page == "Snowflake History":
    from dashboards.pages.snowflake_history import render

    render()
