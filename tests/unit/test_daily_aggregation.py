"""Unit tests for the PySpark daily aggregation batch job.

Tests cover indicator functions, signal generation, sector rollups,
correlation matrix, and pipeline orchestration via mock Spark
DataFrames. The _FakeColumn / _make_fake_F pattern is used to avoid
needing a live SparkSession, which crashes on Windows + Python 3.12.
"""

from unittest.mock import MagicMock, patch


from src.batch.daily_aggregation import (
    add_daily_return,
    add_ema,
    add_macd,
    add_rsi,
    add_sector,
    add_sma,
    add_volume_metrics,
    compute_correlation_matrix,
    compute_daily_summaries,
    compute_sector_performance,
    generate_signals,
    read_silver_data,
    run_daily_aggregation,
    write_gold_outputs,
)


# ---------------------------------------------------------------------------
# _FakeColumn / _make_fake_F – established mock pattern
# ---------------------------------------------------------------------------


class _FakeColumn:
    """Mimics PySpark Column operator protocol for unit tests."""

    def __gt__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __ge__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __lt__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __le__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __and__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __or__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __mul__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __rmul__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __sub__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __rsub__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __truediv__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __neg__(self) -> "_FakeColumn":
        return _FakeColumn()

    def __add__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __radd__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __rtruediv__(self, other: object) -> "_FakeColumn":
        return _FakeColumn()

    def __getitem__(self, key: object) -> "_FakeColumn":
        return _FakeColumn()

    def isNotNull(self) -> "_FakeColumn":
        return _FakeColumn()

    def alias(self, name: str) -> "_FakeColumn":
        return _FakeColumn()

    def desc(self) -> "_FakeColumn":
        return _FakeColumn()

    def asc(self) -> "_FakeColumn":
        return _FakeColumn()

    def otherwise(self, value: object) -> "_FakeColumn":
        return _FakeColumn()

    def over(self, window: object) -> "_FakeColumn":
        return _FakeColumn()


def _make_fake_F() -> MagicMock:
    """Build a mock ``pyspark.sql.functions``."""
    mock_F = MagicMock()
    mock_F.col.return_value = _FakeColumn()
    mock_F.lit.return_value = _FakeColumn()
    when_result = MagicMock()
    when_result.otherwise.return_value = _FakeColumn()
    when_result.when.return_value = when_result
    mock_F.when.return_value = when_result
    mock_F.lag.return_value = _FakeColumn()
    mock_F.avg.return_value = _FakeColumn()
    mock_F.count.return_value = _FakeColumn()
    mock_F.year.return_value = _FakeColumn()
    mock_F.date_format.return_value = _FakeColumn()
    mock_F.row_number.return_value = _FakeColumn()
    mock_F.first.return_value = _FakeColumn()
    mock_F.create_map.return_value = _FakeColumn()
    mock_F.array.return_value = _FakeColumn()
    mock_F.array_join.return_value = _FakeColumn()
    mock_F.expr.return_value = _FakeColumn()
    return mock_F


def _make_chainable_df() -> MagicMock:
    """Create a mock DataFrame whose column-modifying methods return itself."""
    df = MagicMock()
    df.withColumn.return_value = df
    df.drop.return_value = df
    df.dropDuplicates.return_value = df
    df.filter.return_value = df
    df.select.return_value = df
    df.groupBy.return_value.agg.return_value = df
    df.groupBy.return_value.pivot.return_value.agg.return_value.orderBy.return_value = (
        df
    )
    df.join.return_value = df
    df.orderBy.return_value = df
    df.columns = ["symbol", "date", "close", "volume"]
    return df


# ---------------------------------------------------------------------------
# read_silver_data tests
# ---------------------------------------------------------------------------


class TestReadSilverData:
    """Tests for reading silver-layer data."""

    def test_reads_parquet(self) -> None:
        """Calls spark.read.parquet with the correct path."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.parquet.return_value = mock_df

        result = read_silver_data(mock_spark, "s3a://bucket/silver/historical")

        mock_spark.read.parquet.assert_called_once_with(
            "s3a://bucket/silver/historical"
        )
        assert result is mock_df


# ---------------------------------------------------------------------------
# add_daily_return tests
# ---------------------------------------------------------------------------


class TestAddDailyReturn:
    """Tests for the daily return column."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_column(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Adds daily_return_pct column via withColumn."""
        df = _make_chainable_df()

        add_daily_return(df)

        df.withColumn.assert_called()
        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "daily_return_pct" in col_names


# ---------------------------------------------------------------------------
# add_sma tests
# ---------------------------------------------------------------------------


class TestAddSma:
    """Tests for the SMA indicator column."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_sma_column(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Adds sma_20 column for period=20."""
        df = _make_chainable_df()

        add_sma(df, 20)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "sma_20" in col_names

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_different_periods(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Period parameter changes the column name."""
        df = _make_chainable_df()

        add_sma(df, 50)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "sma_50" in col_names


# ---------------------------------------------------------------------------
# add_ema tests
# ---------------------------------------------------------------------------


class TestAddEma:
    """Tests for the EMA indicator column."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_ema_column(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Adds ema_12 column for period=12."""
        df = _make_chainable_df()

        add_ema(df, 12)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "ema_12" in col_names


# ---------------------------------------------------------------------------
# add_rsi tests
# ---------------------------------------------------------------------------


class TestAddRsi:
    """Tests for the RSI indicator column."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_rsi_column(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Adds rsi_14 column and cleans up intermediate columns."""
        df = _make_chainable_df()

        add_rsi(df)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "rsi_14" in col_names
        # Intermediate columns should be dropped
        df.drop.assert_called_once_with(
            "_price_change", "_gain", "_loss", "_avg_gain", "_avg_loss"
        )

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_custom_period(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """RSI period can be customized."""
        df = _make_chainable_df()

        add_rsi(df, period=10)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "rsi_10" in col_names


# ---------------------------------------------------------------------------
# add_macd tests
# ---------------------------------------------------------------------------


class TestAddMacd:
    """Tests for the MACD indicator columns."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_macd_columns(self, mock_window: MagicMock, mock_f: MagicMock) -> None:
        """Adds macd_line, macd_signal, macd_histogram columns."""
        df = _make_chainable_df()
        df.columns = ["symbol", "date", "close", "ema_12", "ema_26"]

        add_macd(df)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "macd_line" in col_names
        assert "macd_signal" in col_names
        assert "macd_histogram" in col_names


# ---------------------------------------------------------------------------
# add_volume_metrics tests
# ---------------------------------------------------------------------------


class TestAddVolumeMetrics:
    """Tests for volume metric columns."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_volume_vs_avg(
        self, mock_window: MagicMock, mock_f: MagicMock
    ) -> None:
        """Adds volume_vs_avg column."""
        df = _make_chainable_df()

        add_volume_metrics(df)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "volume_vs_avg" in col_names


# ---------------------------------------------------------------------------
# add_sector tests
# ---------------------------------------------------------------------------


class TestAddSector:
    """Tests for sector column mapping."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    def test_adds_sector_column(self, mock_f: MagicMock) -> None:
        """Adds sector column via create_map."""
        df = _make_chainable_df()

        add_sector(df)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "sector" in col_names


# ---------------------------------------------------------------------------
# generate_signals tests
# ---------------------------------------------------------------------------


class TestGenerateSignals:
    """Tests for signal generation using Spark expressions."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_adds_signals_column(
        self, mock_window: MagicMock, mock_f: MagicMock
    ) -> None:
        """Adds signals column and drops intermediate signals_arr."""
        df = _make_chainable_df()

        generate_signals(df)

        col_names = [c[0][0] for c in df.withColumn.call_args_list]
        assert "signals" in col_names
        # signals_arr intermediate should be dropped
        df.drop.assert_called()


# ---------------------------------------------------------------------------
# compute_sector_performance tests
# ---------------------------------------------------------------------------


class TestComputeSectorPerformance:
    """Tests for sector performance rollups."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_filters_and_groups(
        self, mock_window: MagicMock, mock_f: MagicMock
    ) -> None:
        """Filters null returns/sectors and groups by sector+date."""
        df = _make_chainable_df()
        mock_grouped = MagicMock()
        filtered = MagicMock()
        df.filter.return_value = filtered
        filtered.withColumn.return_value = filtered
        filtered.groupBy.return_value.agg.return_value = mock_grouped

        # Make join chainable
        mock_grouped.join.return_value = mock_grouped

        compute_sector_performance(df)

        df.filter.assert_called_once()

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    @patch("src.batch.daily_aggregation.Window")
    def test_join_top_bottom_performers(
        self, mock_window: MagicMock, mock_f: MagicMock
    ) -> None:
        """Result joins top and bottom performers."""
        df = _make_chainable_df()
        df.columns = ["symbol", "date", "daily_return_pct", "sector"]

        # Setup chain: filter → withColumn → groupBy.agg → join → join
        filtered = MagicMock()
        df.filter.return_value = filtered
        filtered.withColumn.return_value = filtered
        filtered.filter.return_value = filtered
        filtered.select.return_value = filtered
        filtered.groupBy.return_value = MagicMock()

        MagicMock()
        avg_df = MagicMock()
        filtered.groupBy.return_value.agg.return_value = avg_df
        avg_df.join.return_value = avg_df

        compute_sector_performance(df)

        # Should have at least 2 join calls (top + bottom)
        assert avg_df.join.call_count == 2


# ---------------------------------------------------------------------------
# compute_correlation_matrix tests
# ---------------------------------------------------------------------------


class TestComputeCorrelationMatrix:
    """Tests for rolling correlation computation."""

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    def test_empty_pivot_returns_empty(self, mock_f: MagicMock) -> None:
        """Empty pivot produces empty DataFrame with correct schema."""
        import pandas as pd

        mock_spark = MagicMock()
        df = _make_chainable_df()

        # Set up the chain: filter → groupBy → pivot → agg → orderBy
        filtered = MagicMock()
        df.filter.return_value = filtered
        grouped = MagicMock()
        filtered.groupBy.return_value = grouped
        pivoted = MagicMock()
        grouped.pivot.return_value = pivoted
        agged = MagicMock()
        pivoted.agg.return_value = agged
        ordered = MagicMock()
        agged.orderBy.return_value = ordered

        # toPandas returns empty
        ordered.toPandas.return_value = pd.DataFrame()
        empty_result = MagicMock()
        mock_spark.createDataFrame.return_value = empty_result

        compute_correlation_matrix(df, mock_spark, window=30)

        mock_spark.createDataFrame.assert_called_once()

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    def test_single_symbol_returns_empty(self, mock_f: MagicMock) -> None:
        """Single symbol (can't form pairs) returns empty."""
        import pandas as pd

        mock_spark = MagicMock()
        df = _make_chainable_df()

        filtered = MagicMock()
        df.filter.return_value = filtered
        grouped = MagicMock()
        filtered.groupBy.return_value = grouped
        pivoted = MagicMock()
        grouped.pivot.return_value = pivoted
        agged = MagicMock()
        pivoted.agg.return_value = agged
        ordered = MagicMock()
        agged.orderBy.return_value = ordered

        # Only date + 1 symbol column → shape (35, 2), less than 3
        ordered.toPandas.return_value = pd.DataFrame(
            {"date": list(range(35)), "A": list(range(35))}
        )
        empty_result = MagicMock()
        mock_spark.createDataFrame.return_value = empty_result

        compute_correlation_matrix(df, mock_spark, window=30)

        mock_spark.createDataFrame.assert_called_once()

    @patch("src.batch.daily_aggregation.F", new_callable=_make_fake_F)
    def test_valid_pairs_produces_rows(self, mock_f: MagicMock) -> None:
        """Two symbols with enough data produces correlation rows."""
        import pandas as pd

        mock_spark = MagicMock()
        df = _make_chainable_df()

        filtered = MagicMock()
        df.filter.return_value = filtered
        grouped = MagicMock()
        filtered.groupBy.return_value = grouped
        pivoted = MagicMock()
        grouped.pivot.return_value = pivoted
        agged = MagicMock()
        pivoted.agg.return_value = agged
        ordered = MagicMock()
        agged.orderBy.return_value = ordered

        n = 35
        dates = pd.date_range("2024-01-01", periods=n, freq="B")
        ordered.toPandas.return_value = pd.DataFrame(
            {
                "date": dates,
                "A": [float(i) for i in range(n)],
                "B": [float(i) * 0.8 for i in range(n)],
            }
        )
        result_df = MagicMock()
        mock_spark.createDataFrame.return_value = result_df

        compute_correlation_matrix(df, mock_spark, window=30)

        # createDataFrame should be called with non-empty rows
        args = mock_spark.createDataFrame.call_args
        rows = args[0][0]
        assert len(rows) > 0
        # Each row is (date, symbol_a, symbol_b, correlation)
        assert len(rows[0]) == 4


# ---------------------------------------------------------------------------
# compute_daily_summaries tests
# ---------------------------------------------------------------------------


class TestComputeDailySummaries:
    """Tests for the combined daily summaries function."""

    @patch("src.batch.daily_aggregation.generate_signals")
    @patch("src.batch.daily_aggregation.add_sector")
    @patch("src.batch.daily_aggregation.add_volume_metrics")
    @patch("src.batch.daily_aggregation.add_macd")
    @patch("src.batch.daily_aggregation.add_rsi")
    @patch("src.batch.daily_aggregation.add_ema")
    @patch("src.batch.daily_aggregation.add_sma")
    @patch("src.batch.daily_aggregation.add_daily_return")
    def test_calls_all_indicator_functions(
        self,
        mock_return: MagicMock,
        mock_sma: MagicMock,
        mock_ema: MagicMock,
        mock_rsi: MagicMock,
        mock_macd: MagicMock,
        mock_vol: MagicMock,
        mock_sector: MagicMock,
        mock_signals: MagicMock,
    ) -> None:
        """All indicator functions are called in sequence."""
        df = _make_chainable_df()
        # Each function returns its input for chaining
        mock_return.return_value = df
        mock_sma.return_value = df
        mock_ema.return_value = df
        mock_rsi.return_value = df
        mock_macd.return_value = df
        mock_vol.return_value = df
        mock_sector.return_value = df
        mock_signals.return_value = df

        compute_daily_summaries(df)

        mock_return.assert_called_once()
        # SMA called for 3 periods
        assert mock_sma.call_count == 3
        # EMA called for 2 periods
        assert mock_ema.call_count == 2
        mock_rsi.assert_called_once()
        mock_macd.assert_called_once()
        mock_vol.assert_called_once()
        mock_sector.assert_called_once()
        mock_signals.assert_called_once()


# ---------------------------------------------------------------------------
# run_daily_aggregation tests
# ---------------------------------------------------------------------------


class TestRunDailyAggregation:
    """Tests for the top-level aggregation runner."""

    @patch("src.batch.daily_aggregation.compute_correlation_matrix")
    @patch("src.batch.daily_aggregation.compute_sector_performance")
    @patch("src.batch.daily_aggregation.compute_daily_summaries")
    @patch("src.batch.daily_aggregation.read_silver_data")
    def test_returns_all_output_keys(
        self,
        mock_read: MagicMock,
        mock_summaries: MagicMock,
        mock_sector: MagicMock,
        mock_corr: MagicMock,
    ) -> None:
        """Result dict contains daily_summaries, sector_performance, correlations."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        mock_summaries.return_value = mock_df
        mock_sector.return_value = mock_df
        mock_corr.return_value = mock_df

        result = run_daily_aggregation(mock_spark, "s3a://bucket/silver")

        assert "daily_summaries" in result
        assert "sector_performance" in result
        assert "correlations" in result

    @patch("src.batch.daily_aggregation.compute_correlation_matrix")
    @patch("src.batch.daily_aggregation.compute_sector_performance")
    @patch("src.batch.daily_aggregation.compute_daily_summaries")
    @patch("src.batch.daily_aggregation.read_silver_data")
    def test_passes_spark_to_read(
        self,
        mock_read: MagicMock,
        mock_summaries: MagicMock,
        mock_sector: MagicMock,
        mock_corr: MagicMock,
    ) -> None:
        """SparkSession and path are forwarded to read_silver_data."""
        mock_spark = MagicMock()
        mock_read.return_value = MagicMock()
        mock_summaries.return_value = MagicMock()
        mock_sector.return_value = MagicMock()
        mock_corr.return_value = MagicMock()

        run_daily_aggregation(mock_spark, "s3a://bucket/silver")

        mock_read.assert_called_once_with(mock_spark, "s3a://bucket/silver")


# ---------------------------------------------------------------------------
# write_gold_outputs tests
# ---------------------------------------------------------------------------


class TestWriteGoldOutputs:
    """Tests for gold-layer write."""

    def test_writes_each_output(self) -> None:
        """Each output is written as Parquet."""
        df1 = MagicMock()
        df2 = MagicMock()
        mock_write1 = MagicMock()
        mock_write2 = MagicMock()
        df1.write = mock_write1
        df2.write = mock_write2
        mock_write1.mode.return_value = mock_write1
        mock_write1.option.return_value = mock_write1
        mock_write2.mode.return_value = mock_write2
        mock_write2.option.return_value = mock_write2

        outputs = {"daily_summaries": df1, "sector_performance": df2}
        write_gold_outputs(outputs, "s3a://bucket/gold")

        mock_write1.parquet.assert_called_once_with("s3a://bucket/gold/daily_summaries")
        mock_write2.parquet.assert_called_once_with(
            "s3a://bucket/gold/sector_performance"
        )

    def test_overwrite_with_snappy(self) -> None:
        """Outputs use overwrite mode and snappy compression."""
        df = MagicMock()
        mock_write = MagicMock()
        df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write

        write_gold_outputs({"summaries": df}, "s3a://bucket/gold")

        mock_write.mode.assert_called_with("overwrite")
        mock_write.option.assert_called_with("compression", "snappy")
