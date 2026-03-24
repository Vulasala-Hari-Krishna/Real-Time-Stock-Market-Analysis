"""Pydantic v2 models for stock market data.

Defines the canonical data shapes flowing through the pipeline:
Bronze (raw) → Silver (cleaned + indicators) → Gold (aggregated).
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class StockTick(BaseModel):
    """A single real-time price quote from the API.

    Attributes:
        symbol: Ticker symbol (e.g. "AAPL").
        price: Current trading price.
        volume: Number of shares in the latest trade/quote.
        timestamp: UTC time the quote was captured.
        source: Data provider name (default "alpha_vantage").
    """

    symbol: str = Field(..., min_length=1, max_length=10)
    price: float = Field(..., gt=0)
    volume: int = Field(..., ge=0)
    timestamp: datetime
    source: str = Field(default="alpha_vantage")

    @field_validator("symbol")
    @classmethod
    def symbol_uppercase(cls, v: str) -> str:
        """Ensure ticker symbol is always uppercase.

        Args:
            v: Raw symbol string.

        Returns:
            Uppercased symbol string.
        """
        return v.upper()


class OHLCVRecord(BaseModel):
    """Open-High-Low-Close-Volume bar for a single time period.

    Attributes:
        symbol: Ticker symbol.
        date: Trading date.
        open: Opening price.
        high: Period high price.
        low: Period low price.
        close: Closing price.
        volume: Total shares traded.
        source: Data provider name.
    """

    symbol: str = Field(..., min_length=1, max_length=10)
    date: datetime
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: int = Field(..., ge=0)
    source: str = Field(default="yfinance")

    @field_validator("symbol")
    @classmethod
    def symbol_uppercase(cls, v: str) -> str:
        """Ensure ticker symbol is always uppercase.

        Args:
            v: Raw symbol string.

        Returns:
            Uppercased symbol string.
        """
        return v.upper()

    @model_validator(mode="after")
    def high_gte_low(self) -> "OHLCVRecord":
        """Validate that high price is not less than low price.

        Returns:
            The validated model instance.

        Raises:
            ValueError: If high is less than low.
        """
        if self.high < self.low:
            raise ValueError("high must be >= low")
        return self


class TechnicalIndicators(BaseModel):
    """Computed technical indicators for a single bar.

    Attributes:
        symbol: Ticker symbol.
        date: Indicator computation date.
        sma_20: 20-period Simple Moving Average.
        sma_50: 50-period Simple Moving Average.
        sma_200: 200-period Simple Moving Average.
        ema_12: 12-period Exponential Moving Average.
        ema_26: 26-period Exponential Moving Average.
        rsi_14: 14-period Relative Strength Index.
        macd_line: MACD line value.
        macd_signal: MACD signal line value.
        macd_histogram: MACD histogram value.
        bb_upper: Bollinger Band upper band.
        bb_middle: Bollinger Band middle band (SMA-20).
        bb_lower: Bollinger Band lower band.
        volume_anomaly: True if volume exceeds 2x 20-period average.
        golden_cross: True if SMA-50 crossed above SMA-200.
        death_cross: True if SMA-50 crossed below SMA-200.
    """

    symbol: str = Field(..., min_length=1, max_length=10)
    date: datetime
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    rsi_14: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    volume_anomaly: bool = False
    golden_cross: bool = False
    death_cross: bool = False

    @field_validator("rsi_14")
    @classmethod
    def rsi_in_range(cls, v: Optional[float]) -> Optional[float]:
        """Validate RSI is within 0-100 range.

        Args:
            v: RSI value.

        Returns:
            Validated RSI value.

        Raises:
            ValueError: If RSI is outside 0-100.
        """
        if v is not None and (v < 0 or v > 100):
            raise ValueError("RSI must be between 0 and 100")
        return v


class DailySummary(BaseModel):
    """Gold-layer daily summary for a single stock.

    Attributes:
        symbol: Ticker symbol.
        date: Trading date.
        open: Opening price.
        high: Day high price.
        low: Day low price.
        close: Closing price.
        volume: Total shares traded.
        daily_return_pct: Percentage change from previous close.
        sma_20: 20-day Simple Moving Average.
        sma_50: 50-day Simple Moving Average.
        rsi_14: 14-day RSI.
        volume_anomaly: Whether volume was anomalous.
        sector: GICS sector classification.
    """

    symbol: str = Field(..., min_length=1, max_length=10)
    date: datetime
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: int = Field(..., ge=0)
    daily_return_pct: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    rsi_14: Optional[float] = None
    volume_anomaly: bool = False
    sector: Optional[str] = None


class FundamentalData(BaseModel):
    """Company fundamental data from Yahoo Finance.

    Attributes:
        symbol: Ticker symbol.
        retrieved_at: Timestamp when the data was fetched.
        market_cap: Total market capitalization in USD.
        pe_ratio: Price-to-Earnings ratio (trailing).
        forward_pe: Forward Price-to-Earnings ratio.
        dividend_yield: Annual dividend yield as a decimal.
        eps: Earnings per share (trailing).
        beta: Stock beta relative to market.
        fifty_two_week_high: 52-week high price.
        fifty_two_week_low: 52-week low price.
        sector: GICS sector classification.
        industry: Specific industry within the sector.
    """

    symbol: str = Field(..., min_length=1, max_length=10)
    retrieved_at: datetime
    market_cap: Optional[float] = Field(default=None, ge=0)
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    dividend_yield: Optional[float] = Field(default=None, ge=0)
    eps: Optional[float] = None
    beta: Optional[float] = None
    fifty_two_week_high: Optional[float] = Field(default=None, gt=0)
    fifty_two_week_low: Optional[float] = Field(default=None, gt=0)
    sector: Optional[str] = None
    industry: Optional[str] = None
