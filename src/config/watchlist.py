"""Stock watchlist configuration.

Defines the list of stocks monitored by the pipeline. Each entry contains
the ticker symbol, company name, and sector classification.
"""

from typing import TypedDict


class StockInfo(TypedDict):
    """Metadata for a single stock in the watchlist.

    Attributes:
        symbol: Ticker symbol (e.g. "AAPL").
        name: Full company name.
        sector: GICS sector classification.
    """

    symbol: str
    name: str
    sector: str


WATCHLIST: list[StockInfo] = [
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

SYMBOLS: list[str] = [stock["symbol"] for stock in WATCHLIST]
