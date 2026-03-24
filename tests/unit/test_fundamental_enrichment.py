"""Unit tests for the PySpark fundamental enrichment batch job.

yfinance Ticker is mocked at the edge. Spark DataFrames are mocked
to verify correct join and write operations without a live cluster.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.batch.fundamental_enrichment import (
    FUNDAMENTAL_FIELDS,
    FUNDAMENTALS_SCHEMA,
    enrich_with_fundamentals,
    fetch_all_fundamentals,
    fetch_fundamentals,
    fundamentals_to_spark,
    run_fundamental_enrichment,
    write_enriched_gold,
    write_fundamentals_gold,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_ticker_info() -> dict:
    """Simulated yfinance Ticker.info dictionary."""
    return {
        "marketCap": 2_800_000_000_000,
        "trailingPE": 28.5,
        "forwardPE": 26.0,
        "dividendYield": 0.005,
        "trailingEps": 6.25,
        "beta": 1.2,
        "fiftyTwoWeekHigh": 199.62,
        "fiftyTwoWeekLow": 143.90,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestFundamentalsSchema:
    """Tests for the explicit Spark schema."""

    def test_field_count(self) -> None:
        """Schema has 12 fields."""
        assert len(FUNDAMENTALS_SCHEMA.fields) == 12

    def test_required_fields(self) -> None:
        """Schema contains all expected field names."""
        names = {f.name for f in FUNDAMENTALS_SCHEMA.fields}
        expected = {
            "symbol", "market_cap", "pe_ratio", "forward_pe",
            "dividend_yield", "eps", "beta", "fifty_two_week_high",
            "fifty_two_week_low", "yf_sector", "industry", "retrieved_at",
        }
        assert names == expected


# ---------------------------------------------------------------------------
# Fetch fundamentals tests
# ---------------------------------------------------------------------------


class TestFetchFundamentals:
    """Tests for fetching individual symbol fundamentals."""

    @patch("src.batch.fundamental_enrichment.yf.Ticker")
    def test_successful_fetch(
        self, mock_ticker_cls: MagicMock, mock_ticker_info: dict
    ) -> None:
        """All fields are extracted correctly from Ticker.info."""
        mock_ticker_cls.return_value.info = mock_ticker_info

        result = fetch_fundamentals("AAPL")

        assert result["symbol"] == "AAPL"
        assert result["market_cap"] == 2_800_000_000_000
        assert result["pe_ratio"] == 28.5
        assert result["yf_sector"] == "Technology"
        assert "retrieved_at" in result

    @patch("src.batch.fundamental_enrichment.yf.Ticker")
    def test_missing_fields(self, mock_ticker_cls: MagicMock) -> None:
        """Missing fields in Ticker.info are returned as None."""
        mock_ticker_cls.return_value.info = {"marketCap": 1e12}

        result = fetch_fundamentals("AAPL")

        assert result["market_cap"] == 1e12
        assert result["pe_ratio"] is None
        assert result["yf_sector"] is None

    @patch("src.batch.fundamental_enrichment.yf.Ticker")
    def test_empty_info(self, mock_ticker_cls: MagicMock) -> None:
        """Empty Ticker.info returns all None values."""
        mock_ticker_cls.return_value.info = {}

        result = fetch_fundamentals("AAPL")

        assert result["symbol"] == "AAPL"
        for our_key in FUNDAMENTAL_FIELDS.values():
            assert result[our_key] is None

    @patch("src.batch.fundamental_enrichment.yf.Ticker")
    def test_none_info(self, mock_ticker_cls: MagicMock) -> None:
        """None Ticker.info returns all None values."""
        mock_ticker_cls.return_value.info = None

        result = fetch_fundamentals("AAPL")

        assert result["symbol"] == "AAPL"

    @patch("src.batch.fundamental_enrichment.yf.Ticker")
    def test_symbol_uppercased(self, mock_ticker_cls: MagicMock) -> None:
        """Symbol in result is always uppercased."""
        mock_ticker_cls.return_value.info = {}

        result = fetch_fundamentals("aapl")

        assert result["symbol"] == "AAPL"


class TestFetchAllFundamentals:
    """Tests for fetching fundamentals for multiple symbols."""

    @patch("src.batch.fundamental_enrichment.fetch_fundamentals")
    def test_fetches_each_symbol(self, mock_fetch: MagicMock) -> None:
        """Calls fetch_fundamentals for each symbol and returns list."""
        mock_fetch.return_value = {"symbol": "AAPL", "retrieved_at": "2024-01-01"}

        result = fetch_all_fundamentals(["AAPL", "MSFT"])

        assert mock_fetch.call_count == 2
        assert len(result) == 2
        assert isinstance(result, list)

    @patch("src.batch.fundamental_enrichment.fetch_fundamentals")
    def test_exception_adds_placeholder(self, mock_fetch: MagicMock) -> None:
        """Exception for one symbol doesn't block others."""
        mock_fetch.side_effect = [
            RuntimeError("API error"),
            {"symbol": "MSFT", "retrieved_at": "2024-01-01"},
        ]

        result = fetch_all_fundamentals(["AAPL", "MSFT"])

        assert len(result) == 2
        # AAPL placeholder has symbol set
        assert result[0]["symbol"] == "AAPL"

    @patch("src.batch.fundamental_enrichment.fetch_fundamentals")
    def test_empty_list(self, mock_fetch: MagicMock) -> None:
        """Empty symbol list returns empty list."""
        result = fetch_all_fundamentals([])

        assert result == []
        mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# fundamentals_to_spark tests
# ---------------------------------------------------------------------------


class TestFundamentalsToSpark:
    """Tests for converting fetched records to Spark DataFrame."""

    def test_creates_dataframe_with_schema(self) -> None:
        """createDataFrame is called with FUNDAMENTALS_SCHEMA."""
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        records = [{"symbol": "AAPL", "market_cap": 2.8e12, "retrieved_at": "2024"}]
        fundamentals_to_spark(mock_spark, records)

        mock_spark.createDataFrame.assert_called_once()
        _, kwargs = mock_spark.createDataFrame.call_args
        assert kwargs.get("schema") is FUNDAMENTALS_SCHEMA

    def test_empty_records(self) -> None:
        """Empty records list creates an empty DataFrame."""
        mock_spark = MagicMock()
        empty_df = MagicMock()
        mock_spark.createDataFrame.return_value = empty_df

        fundamentals_to_spark(mock_spark, [])

        mock_spark.createDataFrame.assert_called_once_with(
            [], FUNDAMENTALS_SCHEMA
        )

    def test_fills_missing_columns(self) -> None:
        """Records missing schema columns get None-filled."""
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        # Record with only symbol and retrieved_at
        records = [{"symbol": "AAPL", "retrieved_at": "2024"}]
        fundamentals_to_spark(mock_spark, records)

        args = mock_spark.createDataFrame.call_args
        pdf_arg = args[0][0]
        # All 12 schema columns should be present
        assert len(pdf_arg.columns) == 12


# ---------------------------------------------------------------------------
# Enrich tests
# ---------------------------------------------------------------------------


class TestEnrichWithFundamentals:
    """Tests for the Spark join operation."""

    def test_join_on_symbol(self) -> None:
        """Left join is performed on 'symbol' column."""
        price_df = MagicMock()
        price_df.columns = ["symbol", "date", "close"]
        fund_df = MagicMock()
        fund_df.columns = ["symbol", "market_cap", "pe_ratio"]
        price_df.join.return_value = MagicMock()

        enrich_with_fundamentals(price_df, fund_df)

        price_df.join.assert_called_once_with(fund_df, on="symbol", how="left")

    def test_drops_overlapping_columns(self) -> None:
        """Overlapping columns (except symbol) are dropped from fundamentals."""
        price_df = MagicMock()
        price_df.columns = ["symbol", "date", "close", "sector"]
        fund_df = MagicMock()
        fund_df.columns = ["symbol", "market_cap", "sector"]
        fund_df.drop.return_value = fund_df
        price_df.join.return_value = MagicMock()

        enrich_with_fundamentals(price_df, fund_df)

        fund_df.drop.assert_called_once_with("sector")

    def test_no_overlap_no_drop(self) -> None:
        """No overlapping columns means no drop call."""
        price_df = MagicMock()
        price_df.columns = ["symbol", "date", "close"]
        fund_df = MagicMock()
        fund_df.columns = ["symbol", "market_cap"]
        price_df.join.return_value = MagicMock()

        enrich_with_fundamentals(price_df, fund_df)

        fund_df.drop.assert_not_called()


# ---------------------------------------------------------------------------
# Write tests
# ---------------------------------------------------------------------------


class TestWriteFundamentalsGold:
    """Tests for writing fundamentals to gold layer."""

    def test_writes_parquet_snappy(self) -> None:
        """Writes with overwrite + snappy compression."""
        df = MagicMock()
        mock_write = MagicMock()
        df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write

        write_fundamentals_gold(df, "s3a://bucket/gold")

        mock_write.mode.assert_called_with("overwrite")
        mock_write.option.assert_called_with("compression", "snappy")
        mock_write.parquet.assert_called_once_with(
            "s3a://bucket/gold/fundamentals"
        )


class TestWriteEnrichedGold:
    """Tests for writing enriched data to gold layer."""

    def test_writes_parquet_snappy(self) -> None:
        """Writes with overwrite + snappy compression."""
        df = MagicMock()
        mock_write = MagicMock()
        df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write

        write_enriched_gold(df, "s3a://bucket/gold")

        mock_write.mode.assert_called_with("overwrite")
        mock_write.option.assert_called_with("compression", "snappy")
        mock_write.parquet.assert_called_once_with(
            "s3a://bucket/gold/enriched_prices"
        )


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------


class TestRunFundamentalEnrichment:
    """Tests for the full enrichment pipeline."""

    @patch("src.batch.fundamental_enrichment.enrich_with_fundamentals")
    @patch("src.batch.fundamental_enrichment.fundamentals_to_spark")
    @patch("src.batch.fundamental_enrichment.fetch_all_fundamentals")
    def test_returns_both_outputs(
        self,
        mock_fetch: MagicMock,
        mock_to_spark: MagicMock,
        mock_enrich: MagicMock,
    ) -> None:
        """Pipeline returns 'fundamentals' and 'enriched' DataFrames."""
        mock_spark = MagicMock()
        mock_price_df = MagicMock()

        mock_fetch.return_value = [{"symbol": "AAPL"}]
        fund_df = MagicMock()
        mock_to_spark.return_value = fund_df
        enriched_df = MagicMock()
        mock_enrich.return_value = enriched_df

        result = run_fundamental_enrichment(
            mock_spark, mock_price_df, symbols=["AAPL"]
        )

        assert "fundamentals" in result
        assert "enriched" in result
        assert result["fundamentals"] is fund_df
        assert result["enriched"] is enriched_df

    @patch("src.batch.fundamental_enrichment.enrich_with_fundamentals")
    @patch("src.batch.fundamental_enrichment.fundamentals_to_spark")
    @patch("src.batch.fundamental_enrichment.fetch_all_fundamentals")
    def test_passes_spark_to_converter(
        self,
        mock_fetch: MagicMock,
        mock_to_spark: MagicMock,
        mock_enrich: MagicMock,
    ) -> None:
        """SparkSession is forwarded to fundamentals_to_spark."""
        mock_spark = MagicMock()
        mock_price_df = MagicMock()
        mock_fetch.return_value = [{"symbol": "AAPL"}]
        mock_to_spark.return_value = MagicMock()
        mock_enrich.return_value = MagicMock()

        run_fundamental_enrichment(mock_spark, mock_price_df, symbols=["AAPL"])

        mock_to_spark.assert_called_once_with(mock_spark, [{"symbol": "AAPL"}])

    @patch("src.batch.fundamental_enrichment.enrich_with_fundamentals")
    @patch("src.batch.fundamental_enrichment.fundamentals_to_spark")
    @patch("src.batch.fundamental_enrichment.fetch_all_fundamentals")
    def test_enriches_price_data(
        self,
        mock_fetch: MagicMock,
        mock_to_spark: MagicMock,
        mock_enrich: MagicMock,
    ) -> None:
        """enrich_with_fundamentals is called with price + fundamentals."""
        mock_spark = MagicMock()
        mock_price_df = MagicMock()
        fund_df = MagicMock()
        mock_fetch.return_value = []
        mock_to_spark.return_value = fund_df
        mock_enrich.return_value = MagicMock()

        run_fundamental_enrichment(mock_spark, mock_price_df, symbols=["AAPL"])

        mock_enrich.assert_called_once_with(mock_price_df, fund_df)
