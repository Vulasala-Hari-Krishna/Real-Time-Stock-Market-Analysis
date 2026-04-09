"""Unit tests for the PySpark historical backfill batch job.

All external I/O (yfinance, Spark, S3) is mocked. Tests cover
download, pandas→Spark conversion, transformation, write, and
error handling.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.batch.historical_backfill import (
    BACKFILL_YEARS,
    OHLCV_SCHEMA,
    backfill_symbol,
    download_history,
    pandas_to_spark,
    run_backfill,
    transform_to_silver,
    write_bronze,
    write_silver,
)


# ---------------------------------------------------------------------------
# _FakeColumn / _make_fake_F — reuse the established mock pattern
# ---------------------------------------------------------------------------


class _FakeColumn:
    """Mimics PySpark Column operator protocol for unit tests."""

    def __gt__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __ge__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __and__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __or__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __mul__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __rmul__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def isNotNull(self) -> "_FakeColumn":
        return _FakeColumn()

    def alias(self, name: str) -> "_FakeColumn":
        return _FakeColumn()


def _make_fake_F() -> MagicMock:
    """Build a mock ``pyspark.sql.functions``."""
    mock_F = MagicMock()
    mock_F.col.return_value = _FakeColumn()
    mock_F.when.return_value = MagicMock(
        otherwise=MagicMock(return_value=_FakeColumn())
    )
    mock_F.lit.return_value = _FakeColumn()
    mock_F.year.return_value = _FakeColumn()
    mock_F.date_format.return_value = _FakeColumn()
    return mock_F


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_yfinance_df() -> pd.DataFrame:
    """Simulates a DataFrame returned by yfinance.download."""
    dates = pd.date_range("2023-01-01", periods=5, freq="B")
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": [150.0, 151.0, 152.0, 153.0, 154.0],
            "High": [155.0, 156.0, 157.0, 158.0, 159.0],
            "Low": [149.0, 150.0, 151.0, 152.0, 153.0],
            "Close": [153.0, 154.0, 155.0, 156.0, 157.0],
            "Volume": [1000000, 1100000, 1200000, 1300000, 1400000],
        }
    )


@pytest.fixture()
def sample_pandas_df() -> pd.DataFrame:
    """Two-month pandas DataFrame as produced by download_history."""
    dates = [
        datetime(2023, 1, 15, tzinfo=timezone.utc),
        datetime(2023, 1, 16, tzinfo=timezone.utc),
        datetime(2023, 2, 1, tzinfo=timezone.utc),
        datetime(2023, 2, 2, tzinfo=timezone.utc),
    ]
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * 4,
            "date": dates,
            "open": [150.0, 151.0, 152.0, 153.0],
            "high": [155.0, 156.0, 157.0, 158.0],
            "low": [149.0, 150.0, 151.0, 152.0],
            "close": [153.0, 154.0, 155.0, 156.0],
            "volume": [1000000, 1100000, 1200000, 1300000],
        }
    )


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestOHLCVSchema:
    """Tests for the explicit OHLCV schema definition."""

    def test_field_count(self) -> None:
        """Schema has 8 fields."""
        assert len(OHLCV_SCHEMA.fields) == 8

    def test_required_fields(self) -> None:
        """Schema contains expected field names."""
        names = {f.name for f in OHLCV_SCHEMA.fields}
        expected = {
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        }
        assert names == expected


# ---------------------------------------------------------------------------
# Download tests
# ---------------------------------------------------------------------------


class TestDownloadHistory:
    """Tests for the yfinance download wrapper."""

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_successful_download(
        self,
        mock_download: MagicMock,
        _mock_api: MagicMock,
        sample_yfinance_df: pd.DataFrame,
    ) -> None:
        """Successful download returns DataFrame with symbol column."""
        mock_download.return_value = sample_yfinance_df

        result = download_history("AAPL")

        assert not result.empty
        assert "symbol" in result.columns
        assert (result["symbol"] == "AAPL").all()
        mock_download.assert_called_once_with(
            "AAPL", period=f"{BACKFILL_YEARS}y", auto_adjust=True, progress=False
        )

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_empty_download(
        self, mock_download: MagicMock, _mock_api: MagicMock
    ) -> None:
        """Empty download returns empty DataFrame."""
        mock_download.return_value = pd.DataFrame()

        result = download_history("INVALID")

        assert result.empty

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_none_download(
        self, mock_download: MagicMock, _mock_api: MagicMock
    ) -> None:
        """None download returns empty DataFrame."""
        mock_download.return_value = None

        result = download_history("INVALID")

        assert result.empty

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_unexpected_columns(
        self, mock_download: MagicMock, _mock_api: MagicMock
    ) -> None:
        """DataFrame with missing columns returns empty."""
        mock_download.return_value = pd.DataFrame({"Foo": [1, 2], "Bar": [3, 4]})

        result = download_history("AAPL")

        assert result.empty

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_custom_years(
        self,
        mock_download: MagicMock,
        _mock_api: MagicMock,
        sample_yfinance_df: pd.DataFrame,
    ) -> None:
        """Custom year parameter is passed to yfinance."""
        mock_download.return_value = sample_yfinance_df

        download_history("AAPL", years=3)

        mock_download.assert_called_once_with(
            "AAPL", period="3y", auto_adjust=True, progress=False
        )

    @patch(
        "src.batch.historical_backfill._download_via_api", return_value=pd.DataFrame()
    )
    @patch("src.batch.historical_backfill.yf.download")
    def test_column_normalisation(
        self,
        mock_download: MagicMock,
        _mock_api: MagicMock,
        sample_yfinance_df: pd.DataFrame,
    ) -> None:
        """Column names are normalised to lowercase."""
        mock_download.return_value = sample_yfinance_df

        result = download_history("AAPL")

        for col in ("date", "open", "high", "low", "close", "volume"):
            assert col in result.columns


# ---------------------------------------------------------------------------
# pandas_to_spark tests
# ---------------------------------------------------------------------------


class TestPandasToSpark:
    """Tests for pandas → Spark conversion."""

    def test_calls_create_dataframe(self, sample_pandas_df: pd.DataFrame) -> None:
        """createDataFrame is called with the OHLCV schema."""
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        pandas_to_spark(mock_spark, sample_pandas_df)

        mock_spark.createDataFrame.assert_called_once()
        _, kwargs = mock_spark.createDataFrame.call_args
        assert kwargs.get("schema") is OHLCV_SCHEMA

    def test_adds_source_column(self, sample_pandas_df: pd.DataFrame) -> None:
        """Source column is set to 'yfinance' before conversion."""
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        pandas_to_spark(mock_spark, sample_pandas_df)

        args = mock_spark.createDataFrame.call_args
        pdf_arg = args[0][0]
        assert "source" in pdf_arg.columns
        assert (pdf_arg["source"] == "yfinance").all()


# ---------------------------------------------------------------------------
# transform_to_silver tests
# ---------------------------------------------------------------------------


class TestTransformToSilver:
    """Tests for the silver transformation (mocked Spark DataFrame)."""

    @patch("src.batch.historical_backfill.F", new_callable=_make_fake_F)
    def test_drops_duplicates(self, mock_f: MagicMock) -> None:
        """Calls dropDuplicates on symbol + date."""
        mock_df = MagicMock()
        deduped = MagicMock()
        mock_df.dropDuplicates.return_value = deduped
        deduped.withColumn.return_value = deduped

        transform_to_silver(mock_df)

        mock_df.dropDuplicates.assert_called_once_with(["symbol", "date"])

    @patch("src.batch.historical_backfill.F", new_callable=_make_fake_F)
    def test_adds_year_column(self, mock_f: MagicMock) -> None:
        """Year partition column is added."""
        mock_df = MagicMock()
        mock_df.dropDuplicates.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        transform_to_silver(mock_df)

        # First withColumn calls: "year", then "month"
        calls = mock_df.withColumn.call_args_list
        col_names = [c[0][0] for c in calls]
        assert "year" in col_names
        assert "month" in col_names


# ---------------------------------------------------------------------------
# write_bronze / write_silver tests
# ---------------------------------------------------------------------------


class TestWriteBronze:
    """Tests for bronze-layer write."""

    @patch("src.batch.historical_backfill.F", new_callable=_make_fake_F)
    def test_writes_json_partitioned(self, mock_f: MagicMock) -> None:
        """Writes JSON partitioned by symbol/year/month."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write

        write_bronze(mock_df, "s3a://bucket/bronze/historical")

        mock_write.mode.assert_called_with("overwrite")
        mock_write.partitionBy.assert_called_with("symbol", "year", "month")
        mock_write.json.assert_called_once()


class TestWriteSilver:
    """Tests for silver-layer write."""

    def test_writes_parquet_snappy(self) -> None:
        """Writes Parquet with snappy compression."""
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write
        mock_write.option.return_value = mock_write

        write_silver(mock_df, "s3a://bucket/silver/historical")

        mock_write.mode.assert_called_with("overwrite")
        mock_write.partitionBy.assert_called_with("symbol", "year", "month")
        mock_write.option.assert_called_with("compression", "snappy")
        mock_write.parquet.assert_called_once()


# ---------------------------------------------------------------------------
# Backfill symbol tests
# ---------------------------------------------------------------------------


class TestBackfillSymbol:
    """Tests for the per-symbol backfill orchestration."""

    @patch("src.batch.historical_backfill.write_silver")
    @patch("src.batch.historical_backfill.write_bronze")
    @patch("src.batch.historical_backfill.transform_to_silver")
    @patch("src.batch.historical_backfill.pandas_to_spark")
    @patch("src.batch.historical_backfill.download_history")
    def test_successful_backfill(
        self,
        mock_download: MagicMock,
        mock_p2s: MagicMock,
        mock_transform: MagicMock,
        mock_wb: MagicMock,
        mock_ws: MagicMock,
        sample_pandas_df: pd.DataFrame,
    ) -> None:
        """Successful backfill returns True and calls pipeline steps."""
        mock_download.return_value = sample_pandas_df
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_p2s.return_value = mock_sdf
        mock_transform.return_value = mock_sdf

        result = backfill_symbol(mock_spark, "AAPL", "bronze_path", "silver_path")

        assert result is True
        mock_download.assert_called_once_with("AAPL")
        mock_p2s.assert_called_once_with(mock_spark, sample_pandas_df)
        mock_wb.assert_called_once()
        mock_transform.assert_called_once()
        mock_ws.assert_called_once()

    @patch("src.batch.historical_backfill.download_history")
    def test_empty_download_returns_false(self, mock_download: MagicMock) -> None:
        """Empty download causes backfill to return False."""
        mock_download.return_value = pd.DataFrame()
        mock_spark = MagicMock()

        result = backfill_symbol(mock_spark, "AAPL", "bronze", "silver")

        assert result is False


# ---------------------------------------------------------------------------
# Run backfill tests
# ---------------------------------------------------------------------------


class TestRunBackfill:
    """Tests for the full backfill orchestration."""

    @patch("src.batch.historical_backfill.get_settings")
    @patch("src.batch.historical_backfill.backfill_symbol")
    def test_runs_for_all_symbols(
        self, mock_backfill: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Backfill runs for each provided symbol."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_settings.return_value.aws_default_region = "us-east-1"
        mock_backfill.return_value = True
        mock_spark = MagicMock()

        results = run_backfill(spark=mock_spark, symbols=["AAPL", "MSFT"])

        assert mock_backfill.call_count == 2
        assert results == {"AAPL": True, "MSFT": True}

    @patch("src.batch.historical_backfill.get_settings")
    @patch("src.batch.historical_backfill.backfill_symbol")
    def test_handles_partial_failure(
        self, mock_backfill: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Partial failures are tracked per symbol."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_settings.return_value.aws_default_region = "us-east-1"
        mock_backfill.side_effect = [True, False]
        mock_spark = MagicMock()

        results = run_backfill(spark=mock_spark, symbols=["AAPL", "MSFT"])

        assert results["AAPL"] is True
        assert results["MSFT"] is False

    @patch("src.batch.historical_backfill.get_settings")
    @patch("src.batch.historical_backfill.backfill_symbol")
    def test_exception_marks_failure(
        self, mock_backfill: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Exception during backfill marks the symbol as failed."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_settings.return_value.aws_default_region = "us-east-1"
        mock_backfill.side_effect = RuntimeError("network error")
        mock_spark = MagicMock()

        results = run_backfill(spark=mock_spark, symbols=["AAPL"])

        assert results["AAPL"] is False

    @patch("src.batch.historical_backfill.create_spark_session")
    @patch("src.batch.historical_backfill.get_settings")
    @patch("src.batch.historical_backfill.backfill_symbol")
    def test_creates_spark_session_when_none(
        self,
        mock_backfill: MagicMock,
        mock_settings: MagicMock,
        mock_create_spark: MagicMock,
    ) -> None:
        """SparkSession is created when not provided."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_settings.return_value.aws_default_region = "us-east-1"
        mock_backfill.return_value = True

        run_backfill(spark=None, symbols=["AAPL"])

        mock_create_spark.assert_called_once()
