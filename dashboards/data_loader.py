"""Shared static watchlist data for the Streamlit dashboard.

All actual data loading now goes through ``dashboards.snowflake_loader``
(historical/aggregate datasets, hybrid Databricks -> Snowflake) and
``dashboards.landing_reader`` (live ticks, straight from S3
``landing/ticks/``) - both of which import the constants below.
"""

WATCHLIST = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Technology"},
    {"symbol": "GOOGL", "name": "Alphabet Inc.", "sector": "Technology"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "META", "name": "Meta Platforms Inc.", "sector": "Technology"},
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology"},
    {"symbol": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Financial Services"},
    {"symbol": "V", "name": "Visa Inc.", "sector": "Financial Services"},
    {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare"},
]
SYMBOLS = [s["symbol"] for s in WATCHLIST]
SECTOR_MAP = {s["symbol"]: s["sector"] for s in WATCHLIST}
NAME_MAP = {s["symbol"]: s["name"] for s in WATCHLIST}
