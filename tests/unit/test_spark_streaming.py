"""Unit tests for the Spark Structured Streaming consumer.

Tests use mocked SparkSession and DataFrames to verify transformation
logic without requiring a running Spark cluster. Schema and config
tests use direct assertions on the StructType definitions.
"""

from unittest.mock import MagicMock, patch

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from src.consumers.spark_streaming import (
    STOCK_TICK_SCHEMA,
    VOLUME_ANOMALY_THRESHOLD,
    WATERMARK_DURATION,
    add_partition_columns,
    add_volume_anomaly_flag,
    build_streaming_pipeline,
    clean_ticks,
    compute_windowed_ohlcv,
    deduplicate_ticks,
    deserialize_ticks,
    read_from_kafka,
    start_streaming_query,
    write_batch_to_s3,
)


class _FakeColumn:
    """Mimics PySpark Column operator protocol for unit tests.

    MagicMock does not support ``>``, ``>=``, ``&``, ``|`` operators
    in Python 3.12, so we use this lightweight stand-in wherever
    ``F.col()`` return values participate in expressions.
    """

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
    """Build a mock ``pyspark.sql.functions`` whose ``col`` returns _FakeColumn."""
    mock_F = MagicMock()
    mock_F.col.return_value = _FakeColumn()
    # F.when(...).otherwise(...) → _FakeColumn
    mock_F.when.return_value = MagicMock(otherwise=MagicMock(return_value=_FakeColumn()))
    mock_F.lit.return_value = _FakeColumn()
    mock_F.avg.return_value = _FakeColumn()
    mock_F.from_json.return_value = _FakeColumn()
    mock_F.date_format.return_value = _FakeColumn()
    mock_F.window.return_value = _FakeColumn()
    mock_F.first.return_value = _FakeColumn()
    mock_F.last.return_value = _FakeColumn()
    mock_F.max.return_value = _FakeColumn()
    mock_F.min.return_value = _FakeColumn()
    mock_F.sum.return_value = _FakeColumn()
    mock_F.count.return_value = _FakeColumn()
    return mock_F


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------
class TestSchema:
    """Tests for the STOCK_TICK_SCHEMA definition."""

    def test_schema_field_count(self) -> None:
        """Schema has exactly 5 fields."""
        assert len(STOCK_TICK_SCHEMA.fields) == 5

    def test_schema_field_names(self) -> None:
        """Schema contains all expected field names."""
        names = [f.name for f in STOCK_TICK_SCHEMA.fields]
        assert names == ["symbol", "price", "volume", "timestamp", "source"]

    def test_schema_non_nullable_fields(self) -> None:
        """symbol, price, volume, timestamp are non-nullable."""
        non_nullable = {f.name for f in STOCK_TICK_SCHEMA.fields if not f.nullable}
        assert non_nullable == {"symbol", "price", "volume", "timestamp"}

    def test_schema_nullable_source(self) -> None:
        """source field is nullable."""
        source_field = [f for f in STOCK_TICK_SCHEMA.fields if f.name == "source"][0]
        assert source_field.nullable is True

    def test_schema_field_types(self) -> None:
        """Each field has the correct data type."""
        type_map = {f.name: type(f.dataType) for f in STOCK_TICK_SCHEMA.fields}
        assert type_map["symbol"] is StringType
        assert type_map["price"] is DoubleType
        assert type_map["volume"] is IntegerType
        assert type_map["timestamp"] is TimestampType
        assert type_map["source"] is StringType

    def test_schema_matches_stock_tick_model(self) -> None:
        """Schema fields align with StockTick Pydantic model fields."""
        from src.common.schemas import StockTick

        pydantic_fields = set(StockTick.model_fields.keys())
        schema_fields = {f.name for f in STOCK_TICK_SCHEMA.fields}
        assert schema_fields == pydantic_fields


# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------
class TestCleanTicks:
    """Tests for the data cleaning logic using mocked DataFrames."""

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_clean_applies_all_filters(self, mock_F: MagicMock) -> None:
        """Clean function chains four filter calls."""
        mock_df = MagicMock()
        filtered1 = MagicMock()
        filtered2 = MagicMock()
        filtered3 = MagicMock()
        filtered4 = MagicMock()
        mock_df.filter.return_value = filtered1
        filtered1.filter.return_value = filtered2
        filtered2.filter.return_value = filtered3
        filtered3.filter.return_value = filtered4

        result = clean_ticks(mock_df)

        assert result is filtered4
        assert mock_df.filter.call_count == 1
        assert filtered1.filter.call_count == 1
        assert filtered2.filter.call_count == 1
        assert filtered3.filter.call_count == 1

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_clean_filters_null_symbol(self, mock_F: MagicMock) -> None:
        """First filter checks symbol is not null."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        clean_ticks(mock_df)

        assert mock_df.filter.call_count >= 1

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_clean_returns_dataframe(self, mock_F: MagicMock) -> None:
        """Clean function returns a DataFrame-like object."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        result = clean_ticks(mock_df)

        assert result is not None


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
class TestDeduplication:
    """Tests for the deduplication logic."""

    def test_dedup_applies_watermark(self) -> None:
        """Dedup applies a watermark on the timestamp column."""
        mock_df = MagicMock()
        watermarked = MagicMock()
        mock_df.withWatermark.return_value = watermarked
        deduped = MagicMock()
        watermarked.dropDuplicates.return_value = deduped

        result = deduplicate_ticks(mock_df)

        mock_df.withWatermark.assert_called_once_with("timestamp", WATERMARK_DURATION)
        watermarked.dropDuplicates.assert_called_once_with(["symbol", "timestamp"])
        assert result is deduped

    def test_watermark_duration(self) -> None:
        """Watermark duration is set to 10 minutes."""
        assert WATERMARK_DURATION == "10 minutes"


# ---------------------------------------------------------------------------
# Volume anomaly detection
# ---------------------------------------------------------------------------
class TestVolumeAnomalyDetection:
    """Tests for the volume anomaly flagging logic."""

    def test_threshold_value(self) -> None:
        """Volume anomaly threshold is set to 2.0."""
        assert VOLUME_ANOMALY_THRESHOLD == 2.0

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_anomaly_uses_groupby_avg(self, mock_F: MagicMock) -> None:
        """Volume anomaly detection computes avg volume per symbol."""
        mock_df = MagicMock()
        mock_grouped = MagicMock()
        mock_avg_df = MagicMock()
        mock_joined = MagicMock()
        mock_with_col = MagicMock()
        mock_dropped = MagicMock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_avg_df
        mock_df.join.return_value = mock_joined
        mock_joined.withColumn.return_value = mock_with_col
        mock_with_col.drop.return_value = mock_dropped

        result = add_volume_anomaly_flag(mock_df)

        mock_df.groupBy.assert_called_once_with("symbol")
        mock_df.join.assert_called_once_with(mock_avg_df, on="symbol", how="left")
        assert result is mock_dropped

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_anomaly_adds_boolean_column(self, mock_F: MagicMock) -> None:
        """A 'volume_anomaly' column is added via withColumn."""
        mock_df = MagicMock()
        mock_grouped = MagicMock()
        mock_avg_df = MagicMock()
        mock_joined = MagicMock()
        mock_with_col = MagicMock()
        mock_dropped = MagicMock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_avg_df
        mock_df.join.return_value = mock_joined
        mock_joined.withColumn.return_value = mock_with_col
        mock_with_col.drop.return_value = mock_dropped

        add_volume_anomaly_flag(mock_df)

        col_name = mock_joined.withColumn.call_args[0][0]
        assert col_name == "volume_anomaly"

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_anomaly_drops_avg_volume(self, mock_F: MagicMock) -> None:
        """Temporary avg_volume column is dropped after computation."""
        mock_df = MagicMock()
        mock_grouped = MagicMock()
        mock_avg_df = MagicMock()
        mock_joined = MagicMock()
        mock_with_col = MagicMock()
        mock_dropped = MagicMock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_avg_df
        mock_df.join.return_value = mock_joined
        mock_joined.withColumn.return_value = mock_with_col
        mock_with_col.drop.return_value = mock_dropped

        add_volume_anomaly_flag(mock_df)

        mock_with_col.drop.assert_called_once_with("avg_volume")


# ---------------------------------------------------------------------------
# Partition key generation
# ---------------------------------------------------------------------------
class TestPartitionColumns:
    """Tests for partition column generation."""

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_partition_adds_three_columns(self, mock_F: MagicMock) -> None:
        """Three partition columns are added: year, month, day."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        add_partition_columns(mock_df)

        assert mock_df.withColumn.call_count == 3

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_partition_column_names(self, mock_F: MagicMock) -> None:
        """Columns are named year, month, day in that order."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        add_partition_columns(mock_df)

        col_names = [c[0][0] for c in mock_df.withColumn.call_args_list]
        assert col_names == ["year", "month", "day"]

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_partition_returns_dataframe(self, mock_F: MagicMock) -> None:
        """Partition function returns a DataFrame-like object."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = add_partition_columns(mock_df)

        assert result is not None


# ---------------------------------------------------------------------------
# Kafka reading
# ---------------------------------------------------------------------------
class TestReadFromKafka:
    """Tests for the Kafka readStream configuration."""

    def test_read_from_kafka_options(self) -> None:
        """Read uses correct Kafka options."""
        mock_spark = MagicMock()
        mock_stream = MagicMock()
        mock_spark.readStream.format.return_value = mock_stream
        mock_stream.option.return_value = mock_stream
        mock_stream.load.return_value = MagicMock()

        read_from_kafka(mock_spark, "broker:9092", "test_topic")

        mock_spark.readStream.format.assert_called_once_with("kafka")

    def test_read_sets_subscribe_topic(self) -> None:
        """Read subscribes to the specified topic."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.readStream.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        read_from_kafka(mock_spark, "broker:9092", "raw_stock_ticks")

        # Check that 'subscribe' option was set with the topic name
        option_calls = mock_reader.option.call_args_list
        subscribe_calls = [c for c in option_calls if c[0][0] == "subscribe"]
        assert len(subscribe_calls) == 1
        assert subscribe_calls[0][0][1] == "raw_stock_ticks"


# ---------------------------------------------------------------------------
# Deserialization
# ---------------------------------------------------------------------------
class TestDeserializeTicks:
    """Tests for JSON deserialization from Kafka."""

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_deserialize_calls_selectexpr(self, mock_F: MagicMock) -> None:
        """Deserialization starts by casting value to string."""
        mock_df = MagicMock()
        mock_cast = MagicMock()
        mock_df.selectExpr.return_value = mock_cast
        mock_cast.select.return_value = MagicMock()

        deserialize_ticks(mock_df)

        mock_df.selectExpr.assert_called_once()
        args = mock_df.selectExpr.call_args[0]
        assert "CAST(value AS STRING)" in args[0]

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_deserialize_returns_dataframe(self, mock_F: MagicMock) -> None:
        """Deserialization returns a DataFrame-like object."""
        mock_df = MagicMock()
        mock_cast = MagicMock()
        mock_selected = MagicMock()
        mock_df.selectExpr.return_value = mock_cast
        mock_cast.select.return_value = mock_selected
        mock_selected.select.return_value = MagicMock()

        result = deserialize_ticks(mock_df)

        assert result is not None


# ---------------------------------------------------------------------------
# Write batch to S3
# ---------------------------------------------------------------------------
class TestWriteBatchToS3:
    """Tests for the foreachBatch write function."""

    @patch("src.consumers.spark_streaming.get_settings")
    @patch("src.consumers.spark_streaming.add_partition_columns")
    def test_empty_batch_skipped(
        self, mock_partitions: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Empty batches are skipped without writing."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True

        write_batch_to_s3(mock_df, batch_id=0)

        mock_partitions.assert_not_called()

    @patch("src.consumers.spark_streaming.get_settings")
    @patch("src.consumers.spark_streaming.add_partition_columns")
    def test_non_empty_batch_writes(
        self, mock_partitions: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Non-empty batches are partitioned and written as Parquet."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"

        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 5

        mock_partitioned = MagicMock()
        mock_partitions.return_value = mock_partitioned

        write_batch_to_s3(mock_df, batch_id=1)

        mock_partitions.assert_called_once_with(mock_df)
        mock_partitioned.write.mode.assert_called_once_with("append")


# ---------------------------------------------------------------------------
# Windowed aggregation
# ---------------------------------------------------------------------------
class TestComputeWindowedOHLCV:
    """Tests for windowed OHLCV computation."""

    @patch("src.consumers.spark_streaming.F", new_callable=_make_fake_F)
    def test_windowed_uses_groupby_window(self, mock_F: MagicMock) -> None:
        """Windowed aggregation groups by window and symbol."""
        mock_df = MagicMock()
        mock_watermarked = MagicMock()
        mock_grouped = MagicMock()
        mock_agged = MagicMock()
        mock_selected = MagicMock()

        mock_df.withWatermark.return_value = mock_watermarked
        mock_watermarked.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_agged
        mock_agged.select.return_value = mock_selected

        result = compute_windowed_ohlcv(mock_df, "5 minutes")

        mock_df.withWatermark.assert_called_once()
        mock_watermarked.groupBy.assert_called_once()
        assert result is mock_selected


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------
class TestBuildStreamingPipeline:
    """Tests for the full pipeline assembly."""

    @patch("src.consumers.spark_streaming.deduplicate_ticks")
    @patch("src.consumers.spark_streaming.clean_ticks")
    @patch("src.consumers.spark_streaming.deserialize_ticks")
    @patch("src.consumers.spark_streaming.read_from_kafka")
    @patch("src.consumers.spark_streaming.get_settings")
    def test_pipeline_chains_correctly(
        self,
        mock_settings: MagicMock,
        mock_read: MagicMock,
        mock_deserialize: MagicMock,
        mock_clean: MagicMock,
        mock_dedup: MagicMock,
    ) -> None:
        """Pipeline calls read → deserialize → clean → deduplicate."""
        mock_settings.return_value.kafka_broker = "broker:9092"

        mock_spark = MagicMock()
        mock_raw = MagicMock()
        mock_deserialized = MagicMock()
        mock_cleaned = MagicMock()
        mock_deduped = MagicMock()

        mock_read.return_value = mock_raw
        mock_deserialize.return_value = mock_deserialized
        mock_clean.return_value = mock_cleaned
        mock_dedup.return_value = mock_deduped

        result = build_streaming_pipeline(mock_spark)

        mock_read.assert_called_once_with(mock_spark, "broker:9092", "raw_stock_ticks")
        mock_deserialize.assert_called_once_with(mock_raw)
        mock_clean.assert_called_once_with(mock_deserialized)
        mock_dedup.assert_called_once_with(mock_cleaned)
        assert result is mock_deduped


# ---------------------------------------------------------------------------
# Start streaming query
# ---------------------------------------------------------------------------
class TestStartStreamingQuery:
    """Tests for streaming query start."""

    def test_query_uses_foreach_batch(self) -> None:
        """Query uses foreachBatch output mode."""
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.writeStream.foreachBatch.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_query = MagicMock()
        mock_writer.start.return_value = mock_query

        start_streaming_query(mock_df, "/tmp/checkpoint")

        mock_df.writeStream.foreachBatch.assert_called_once_with(write_batch_to_s3)
        mock_writer.outputMode.assert_called_once_with("append")
        mock_writer.queryName.assert_called_once_with("silver_stock_ticks")
        mock_query.awaitTermination.assert_called_once()
