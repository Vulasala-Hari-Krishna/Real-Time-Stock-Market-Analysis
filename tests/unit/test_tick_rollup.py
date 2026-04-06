"""Unit tests for the PySpark tick-to-OHLCV rollup batch job.

All external I/O (Spark, S3) is mocked.  Tests cover tick reading,
OHLCV aggregation, deduplication, writing, and pipeline orchestration.
"""

from unittest.mock import MagicMock, patch

from src.batch.tick_rollup import (
    DAILY_OHLCV_SCHEMA,
    deduplicate_against_existing,
    read_tick_data,
    rollup_ticks_to_daily,
    run_tick_rollup,
    write_daily_bars,
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

    def isNotNull(self) -> "_FakeColumn":
        return _FakeColumn()

    def alias(self, name: str) -> "_FakeColumn":
        return _FakeColumn()

    def cast(self, dtype: object) -> "_FakeColumn":
        return _FakeColumn()


def _make_fake_F() -> MagicMock:
    """Build a mock ``pyspark.sql.functions``."""
    mock_F = MagicMock()
    mock_F.col.return_value = _FakeColumn()
    mock_F.to_date.return_value = _FakeColumn()
    mock_F.first.return_value = _FakeColumn()
    mock_F.last.return_value = _FakeColumn()
    mock_F.max.return_value = _FakeColumn()
    mock_F.min.return_value = _FakeColumn()
    mock_F.sum.return_value = _FakeColumn()
    mock_F.lit.return_value = _FakeColumn()
    mock_F.year.return_value = _FakeColumn()
    mock_F.date_format.return_value = _FakeColumn()
    return mock_F


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestDailyOHLCVSchema:
    """Tests for the output schema definition."""

    def test_field_count(self) -> None:
        """Schema has 8 fields."""
        assert len(DAILY_OHLCV_SCHEMA.fields) == 8

    def test_required_fields(self) -> None:
        """Schema contains expected field names."""
        names = {f.name for f in DAILY_OHLCV_SCHEMA.fields}
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
# read_tick_data tests
# ---------------------------------------------------------------------------


class TestReadTickData:
    """Tests for the tick data reader."""

    def test_reads_parquet(self) -> None:
        """Reads Parquet when path exists."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.parquet.return_value = mock_df

        result = read_tick_data(mock_spark, "s3a://bucket/silver/stock_ticks")

        mock_spark.read.parquet.assert_called_once_with(
            "s3a://bucket/silver/stock_ticks"
        )
        assert result is mock_df

    def test_returns_empty_on_missing_path(self) -> None:
        """Returns empty DataFrame when path doesn't exist (first run)."""
        mock_spark = MagicMock()
        mock_spark.read.parquet.side_effect = Exception("Path does not exist")
        mock_empty = MagicMock()
        mock_spark.createDataFrame.return_value = mock_empty

        result = read_tick_data(mock_spark, "s3a://bucket/silver/stock_ticks")

        mock_spark.createDataFrame.assert_called_once_with([], DAILY_OHLCV_SCHEMA)
        assert result is mock_empty


# ---------------------------------------------------------------------------
# rollup_ticks_to_daily tests
# ---------------------------------------------------------------------------


class TestRollupTicksToDaily:
    """Tests for tick-to-daily OHLCV aggregation."""

    def test_empty_df_returns_as_is(self) -> None:
        """Empty DataFrame is returned immediately."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True

        result = rollup_ticks_to_daily(mock_df)

        assert result is mock_df

    @patch("src.batch.tick_rollup.F", new_callable=_make_fake_F)
    def test_groups_by_symbol_and_date(self, mock_f: MagicMock) -> None:
        """Aggregation groups by symbol and calendar date."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.withColumn.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.select.return_value = mock_df

        rollup_ticks_to_daily(mock_df)

        mock_df.groupBy.assert_called_once()
        group_args = mock_df.groupBy.call_args[0]
        # Should group by "symbol" and "date"
        assert "symbol" in group_args
        assert "date" in group_args


# ---------------------------------------------------------------------------
# deduplicate_against_existing tests
# ---------------------------------------------------------------------------


class TestDeduplicateAgainstExisting:
    """Tests for idempotent deduplication."""

    def test_empty_new_df_returns_as_is(self) -> None:
        """Empty new DataFrame is returned immediately."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True
        mock_spark = MagicMock()

        result = deduplicate_against_existing(mock_df, "s3a://path", mock_spark)

        assert result is mock_df

    def test_no_existing_data_returns_all(self) -> None:
        """When no historical data exists, all rows are kept."""
        mock_new = MagicMock()
        mock_new.isEmpty.return_value = False
        mock_spark = MagicMock()
        mock_spark.read.parquet.side_effect = Exception("Path not found")

        result = deduplicate_against_existing(mock_new, "s3a://path", mock_spark)

        assert result is mock_new

    def test_uses_left_anti_join(self) -> None:
        """Uses left_anti join to remove existing rows."""
        mock_new = MagicMock()
        mock_new.isEmpty.return_value = False
        deduped = MagicMock()
        mock_new.join.return_value = deduped
        deduped.count.return_value = 5

        mock_existing = MagicMock()
        mock_keys = MagicMock()
        mock_existing.select.return_value = mock_keys
        mock_keys.distinct.return_value = mock_keys

        mock_spark = MagicMock()
        mock_spark.read.parquet.return_value = mock_existing

        result = deduplicate_against_existing(mock_new, "s3a://path", mock_spark)

        mock_new.join.assert_called_once_with(
            mock_keys, on=["symbol", "date"], how="left_anti"
        )
        assert result is deduped


# ---------------------------------------------------------------------------
# write_daily_bars tests
# ---------------------------------------------------------------------------


class TestWriteDailyBars:
    """Tests for writing daily bars to S3."""

    def test_empty_df_writes_nothing(self) -> None:
        """Empty DataFrame returns 0 without writing."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True

        result = write_daily_bars(mock_df, "s3a://bucket/silver/historical")

        assert result == 0

    @patch("src.batch.tick_rollup.F", new_callable=_make_fake_F)
    def test_writes_with_append_mode(self, mock_f: MagicMock) -> None:
        """Writes Parquet with append mode and snappy compression."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 10
        mock_df.withColumn.return_value = mock_df
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write
        mock_write.option.return_value = mock_write

        result = write_daily_bars(mock_df, "s3a://bucket/silver/historical")

        mock_write.mode.assert_called_with("append")
        mock_write.partitionBy.assert_called_with("symbol", "year", "month")
        mock_write.option.assert_called_with("compression", "snappy")
        mock_write.parquet.assert_called_once()
        assert result == 10


# ---------------------------------------------------------------------------
# run_tick_rollup tests
# ---------------------------------------------------------------------------


class TestRunTickRollup:
    """Tests for the full pipeline orchestration."""

    @patch("src.batch.tick_rollup.write_daily_bars")
    @patch("src.batch.tick_rollup.deduplicate_against_existing")
    @patch("src.batch.tick_rollup.rollup_ticks_to_daily")
    @patch("src.batch.tick_rollup.read_tick_data")
    @patch("src.batch.tick_rollup.get_settings")
    def test_full_pipeline(
        self,
        mock_settings: MagicMock,
        mock_read: MagicMock,
        mock_rollup: MagicMock,
        mock_dedup: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Full pipeline runs all steps in order."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_tick_df = MagicMock()
        mock_daily_df = MagicMock()
        mock_deduped_df = MagicMock()
        mock_read.return_value = mock_tick_df
        mock_rollup.return_value = mock_daily_df
        mock_dedup.return_value = mock_deduped_df
        mock_write.return_value = 5
        mock_spark = MagicMock()

        result = run_tick_rollup(spark=mock_spark)

        mock_read.assert_called_once_with(
            mock_spark, "s3a://test-bucket/silver/stock_ticks"
        )
        mock_rollup.assert_called_once_with(mock_tick_df)
        mock_dedup.assert_called_once_with(
            mock_daily_df, "s3a://test-bucket/silver/historical", mock_spark
        )
        mock_write.assert_called_once_with(
            mock_deduped_df, "s3a://test-bucket/silver/historical"
        )
        assert result == 5

    @patch("src.batch.tick_rollup.create_spark_session")
    @patch("src.batch.tick_rollup.write_daily_bars")
    @patch("src.batch.tick_rollup.deduplicate_against_existing")
    @patch("src.batch.tick_rollup.rollup_ticks_to_daily")
    @patch("src.batch.tick_rollup.read_tick_data")
    @patch("src.batch.tick_rollup.get_settings")
    def test_creates_spark_session_when_none(
        self,
        mock_settings: MagicMock,
        mock_read: MagicMock,
        mock_rollup: MagicMock,
        mock_dedup: MagicMock,
        mock_write: MagicMock,
        mock_create_spark: MagicMock,
    ) -> None:
        """SparkSession is created when not provided."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_read.return_value = MagicMock()
        mock_rollup.return_value = MagicMock()
        mock_dedup.return_value = MagicMock()
        mock_write.return_value = 0

        run_tick_rollup(spark=None)

        mock_create_spark.assert_called_once()

    @patch("src.batch.tick_rollup.write_daily_bars")
    @patch("src.batch.tick_rollup.deduplicate_against_existing")
    @patch("src.batch.tick_rollup.rollup_ticks_to_daily")
    @patch("src.batch.tick_rollup.read_tick_data")
    @patch("src.batch.tick_rollup.get_settings")
    def test_handles_no_ticks(
        self,
        mock_settings: MagicMock,
        mock_read: MagicMock,
        mock_rollup: MagicMock,
        mock_dedup: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Pipeline handles empty tick data gracefully."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_empty = MagicMock()
        mock_empty.isEmpty.return_value = True
        mock_read.return_value = mock_empty
        mock_rollup.return_value = mock_empty
        mock_dedup.return_value = mock_empty
        mock_write.return_value = 0
        mock_spark = MagicMock()

        result = run_tick_rollup(spark=mock_spark)

        assert result == 0
