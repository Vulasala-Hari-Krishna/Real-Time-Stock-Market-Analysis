"""Comprehensive unit tests for Pydantic data models."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.common.schemas import (
    DailySummary,
    FundamentalData,
    OHLCVRecord,
    StockTick,
    TechnicalIndicators,
)


# ---------------------------------------------------------------------------
# StockTick
# ---------------------------------------------------------------------------
class TestStockTick:
    """Tests for the StockTick model."""

    def test_valid_tick(self, sample_tick_data: dict) -> None:
        """A valid tick is created without errors."""
        tick = StockTick(**sample_tick_data)
        assert tick.symbol == "AAPL"
        assert tick.price == 178.50
        assert tick.volume == 52_000_000

    def test_symbol_uppercased(self, sample_tick_data: dict) -> None:
        """Lowercase symbols are automatically uppercased."""
        sample_tick_data["symbol"] = "aapl"
        tick = StockTick(**sample_tick_data)
        assert tick.symbol == "AAPL"

    def test_default_source(self) -> None:
        """Source defaults to 'alpha_vantage'."""
        tick = StockTick(
            symbol="MSFT",
            price=400.0,
            volume=1000,
            timestamp=datetime.now(tz=timezone.utc),
        )
        assert tick.source == "alpha_vantage"

    @pytest.mark.parametrize(
        "field,value,match",
        [
            ("price", 0, "greater than 0"),
            ("price", -10, "greater than 0"),
            ("volume", -1, "greater than or equal to 0"),
            ("symbol", "", "at least 1 character"),
        ],
    )
    def test_invalid_values(
        self, sample_tick_data: dict, field: str, value: object, match: str
    ) -> None:
        """Invalid field values raise ValidationError."""
        sample_tick_data[field] = value
        with pytest.raises(ValidationError, match=match):
            StockTick(**sample_tick_data)

    def test_missing_required_field(self) -> None:
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError):
            StockTick(symbol="AAPL", price=100.0)  # type: ignore[call-arg]

    def test_serialization_roundtrip(self, sample_tick_data: dict) -> None:
        """Model can be serialized to dict and back."""
        tick = StockTick(**sample_tick_data)
        data = tick.model_dump()
        tick2 = StockTick(**data)
        assert tick == tick2

    def test_json_roundtrip(self, sample_tick_data: dict) -> None:
        """Model can be serialized to JSON and back."""
        tick = StockTick(**sample_tick_data)
        json_str = tick.model_dump_json()
        tick2 = StockTick.model_validate_json(json_str)
        assert tick == tick2


# ---------------------------------------------------------------------------
# OHLCVRecord
# ---------------------------------------------------------------------------
class TestOHLCVRecord:
    """Tests for the OHLCVRecord model."""

    def test_valid_record(self, sample_ohlcv_data: dict) -> None:
        """A valid OHLCV record is created without errors."""
        record = OHLCVRecord(**sample_ohlcv_data)
        assert record.symbol == "AAPL"
        assert record.open == 175.00
        assert record.close == 178.50

    def test_symbol_uppercased(self, sample_ohlcv_data: dict) -> None:
        """Lowercase symbols are automatically uppercased."""
        sample_ohlcv_data["symbol"] = "aapl"
        record = OHLCVRecord(**sample_ohlcv_data)
        assert record.symbol == "AAPL"

    def test_high_less_than_low_rejected(self, sample_ohlcv_data: dict) -> None:
        """High < low raises ValidationError."""
        sample_ohlcv_data["high"] = 170.0
        sample_ohlcv_data["low"] = 174.50
        with pytest.raises(ValidationError, match="high must be >= low"):
            OHLCVRecord(**sample_ohlcv_data)

    @pytest.mark.parametrize(
        "field,value",
        [
            ("open", 0),
            ("high", -1),
            ("low", 0),
            ("close", -5),
            ("volume", -1),
        ],
    )
    def test_invalid_numeric_fields(
        self, sample_ohlcv_data: dict, field: str, value: object
    ) -> None:
        """Invalid numeric field values raise ValidationError."""
        sample_ohlcv_data[field] = value
        with pytest.raises(ValidationError):
            OHLCVRecord(**sample_ohlcv_data)

    def test_default_source(self, sample_ohlcv_data: dict) -> None:
        """Source defaults to 'yfinance'."""
        del sample_ohlcv_data["source"]
        record = OHLCVRecord(**sample_ohlcv_data)
        assert record.source == "yfinance"

    def test_serialization_roundtrip(self, sample_ohlcv_data: dict) -> None:
        """Model can be serialized to dict and back."""
        record = OHLCVRecord(**sample_ohlcv_data)
        data = record.model_dump()
        record2 = OHLCVRecord(**data)
        assert record == record2


# ---------------------------------------------------------------------------
# TechnicalIndicators
# ---------------------------------------------------------------------------
class TestTechnicalIndicators:
    """Tests for the TechnicalIndicators model."""

    def test_valid_minimal(self) -> None:
        """A minimal TechnicalIndicators instance with only required fields."""
        ti = TechnicalIndicators(
            symbol="AAPL",
            date=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        assert ti.sma_20 is None
        assert ti.volume_anomaly is False

    def test_all_fields_populated(self) -> None:
        """All optional fields can be set."""
        ti = TechnicalIndicators(
            symbol="AAPL",
            date=datetime(2024, 3, 15, tzinfo=timezone.utc),
            sma_20=170.0,
            sma_50=165.0,
            sma_200=155.0,
            ema_12=172.0,
            ema_26=168.0,
            rsi_14=62.5,
            macd_line=4.0,
            macd_signal=3.5,
            macd_histogram=0.5,
            bb_upper=180.0,
            bb_middle=170.0,
            bb_lower=160.0,
            volume_anomaly=True,
            golden_cross=True,
            death_cross=False,
        )
        assert ti.sma_20 == 170.0
        assert ti.volume_anomaly is True

    @pytest.mark.parametrize("rsi_val", [-1.0, 101.0, -0.01, 100.01])
    def test_rsi_out_of_range(self, rsi_val: float) -> None:
        """RSI outside [0, 100] raises ValidationError."""
        with pytest.raises(ValidationError, match="RSI must be between 0 and 100"):
            TechnicalIndicators(
                symbol="AAPL",
                date=datetime(2024, 3, 15, tzinfo=timezone.utc),
                rsi_14=rsi_val,
            )

    @pytest.mark.parametrize("rsi_val", [0.0, 50.0, 100.0])
    def test_rsi_boundary_values(self, rsi_val: float) -> None:
        """RSI at boundary values (0, 50, 100) are accepted."""
        ti = TechnicalIndicators(
            symbol="AAPL",
            date=datetime(2024, 3, 15, tzinfo=timezone.utc),
            rsi_14=rsi_val,
        )
        assert ti.rsi_14 == rsi_val

    def test_serialization_roundtrip(self) -> None:
        """Model can be serialized and deserialized."""
        ti = TechnicalIndicators(
            symbol="AAPL",
            date=datetime(2024, 3, 15, tzinfo=timezone.utc),
            sma_20=170.0,
            rsi_14=55.0,
        )
        data = ti.model_dump()
        ti2 = TechnicalIndicators(**data)
        assert ti == ti2


# ---------------------------------------------------------------------------
# DailySummary
# ---------------------------------------------------------------------------
class TestDailySummary:
    """Tests for the DailySummary model."""

    def test_valid_summary(self, sample_daily_summary_data: dict) -> None:
        """A valid daily summary is created without errors."""
        summary = DailySummary(**sample_daily_summary_data)
        assert summary.symbol == "AAPL"
        assert summary.daily_return_pct == 1.25
        assert summary.sector == "Technology"

    def test_optional_fields_none(self) -> None:
        """Optional fields default to None/False."""
        summary = DailySummary(
            symbol="MSFT",
            date=datetime(2024, 3, 15, tzinfo=timezone.utc),
            open=400.0,
            high=410.0,
            low=395.0,
            close=405.0,
            volume=30_000_000,
        )
        assert summary.daily_return_pct is None
        assert summary.sma_20 is None
        assert summary.volume_anomaly is False
        assert summary.sector is None

    @pytest.mark.parametrize(
        "field,value",
        [
            ("open", 0),
            ("close", -1),
            ("volume", -1),
        ],
    )
    def test_invalid_values(
        self, sample_daily_summary_data: dict, field: str, value: object
    ) -> None:
        """Invalid values raise ValidationError."""
        sample_daily_summary_data[field] = value
        with pytest.raises(ValidationError):
            DailySummary(**sample_daily_summary_data)

    def test_serialization_roundtrip(self, sample_daily_summary_data: dict) -> None:
        """Model can be serialized and deserialized."""
        summary = DailySummary(**sample_daily_summary_data)
        data = summary.model_dump()
        summary2 = DailySummary(**data)
        assert summary == summary2


# ---------------------------------------------------------------------------
# FundamentalData
# ---------------------------------------------------------------------------
class TestFundamentalData:
    """Tests for the FundamentalData model."""

    def test_valid_fundamental(self, sample_fundamental_data: dict) -> None:
        """A valid fundamental data record is created without errors."""
        fd = FundamentalData(**sample_fundamental_data)
        assert fd.symbol == "AAPL"
        assert fd.market_cap == 2_800_000_000_000.0
        assert fd.sector == "Technology"

    def test_optional_fields_none(self) -> None:
        """Optional fields default to None."""
        fd = FundamentalData(
            symbol="MSFT",
            retrieved_at=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        assert fd.market_cap is None
        assert fd.pe_ratio is None
        assert fd.sector is None

    @pytest.mark.parametrize(
        "field,value",
        [
            ("market_cap", -1),
            ("dividend_yield", -0.01),
            ("fifty_two_week_high", 0),
            ("fifty_two_week_low", -5),
        ],
    )
    def test_invalid_values(
        self, sample_fundamental_data: dict, field: str, value: object
    ) -> None:
        """Invalid values raise ValidationError."""
        sample_fundamental_data[field] = value
        with pytest.raises(ValidationError):
            FundamentalData(**sample_fundamental_data)

    def test_negative_eps_allowed(self, sample_fundamental_data: dict) -> None:
        """Negative EPS is valid (companies can have losses)."""
        sample_fundamental_data["eps"] = -2.50
        fd = FundamentalData(**sample_fundamental_data)
        assert fd.eps == -2.50

    def test_negative_pe_allowed(self, sample_fundamental_data: dict) -> None:
        """Negative P/E is valid (companies with losses)."""
        sample_fundamental_data["pe_ratio"] = -15.0
        fd = FundamentalData(**sample_fundamental_data)
        assert fd.pe_ratio == -15.0

    def test_json_roundtrip(self, sample_fundamental_data: dict) -> None:
        """Model can be serialized to JSON and back."""
        fd = FundamentalData(**sample_fundamental_data)
        json_str = fd.model_dump_json()
        fd2 = FundamentalData.model_validate_json(json_str)
        assert fd == fd2
