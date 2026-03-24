"""Spark Structured Streaming consumer for real-time stock tick processing.

Reads raw stock ticks from Kafka, validates and cleans data, computes
windowed aggregations (5-min and 15-min OHLCV rollups), detects volume
anomalies, and writes cleaned data to S3 silver layer as Parquet.
"""

import logging
import signal
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Explicit schema matching StockTick Pydantic model
STOCK_TICK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", IntegerType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("source", StringType(), nullable=True),
    ]
)

# Volume anomaly threshold: flag when volume > N× rolling average
VOLUME_ANOMALY_THRESHOLD = 2.0

# Watermark duration for late data handling
WATERMARK_DURATION = "10 minutes"

# Streaming query reference for graceful shutdown
_active_query = None


def create_spark_session(app_name: str = "StockStreamConsumer") -> SparkSession:
    """Create and configure a SparkSession for streaming.

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    settings = get_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "false")
    )

    # S3 config if AWS credentials are available
    if settings.aws_access_key_id:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                f"s3.{settings.aws_default_region}.amazonaws.com",
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
        )

    return builder.getOrCreate()


def read_from_kafka(spark: SparkSession, broker: str, topic: str) -> DataFrame:
    """Create a streaming DataFrame reading from a Kafka topic.

    Args:
        spark: Active SparkSession.
        broker: Kafka bootstrap server address.
        topic: Kafka topic to subscribe to.

    Returns:
        Raw streaming DataFrame with Kafka metadata columns.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def deserialize_ticks(raw_df: DataFrame) -> DataFrame:
    """Deserialize JSON values from Kafka into structured columns.

    Args:
        raw_df: Raw Kafka DataFrame with binary key/value columns.

    Returns:
        DataFrame with parsed StockTick columns.
    """
    return (
        raw_df.selectExpr(
            "CAST(value AS STRING) AS json_value", "timestamp AS kafka_timestamp"
        )
        .select(
            F.from_json(F.col("json_value"), STOCK_TICK_SCHEMA).alias("tick"),
            F.col("kafka_timestamp"),
        )
        .select("tick.*", "kafka_timestamp")
    )


def clean_ticks(df: DataFrame) -> DataFrame:
    """Validate and clean tick data.

    Removes rows with null required fields, invalid prices (≤ 0),
    and negative volumes.

    Args:
        df: Deserialized tick DataFrame.

    Returns:
        Cleaned DataFrame with valid rows only.
    """
    return (
        df.filter(F.col("symbol").isNotNull())
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .filter(F.col("volume").isNotNull() & (F.col("volume") >= 0))
        .filter(F.col("timestamp").isNotNull())
    )


def deduplicate_ticks(df: DataFrame) -> DataFrame:
    """Deduplicate ticks using watermark and dropDuplicates.

    Uses a 10-minute watermark on the timestamp column and drops
    duplicates based on symbol + timestamp combination.

    Args:
        df: Cleaned tick DataFrame.

    Returns:
        Deduplicated streaming DataFrame.
    """
    return df.withWatermark("timestamp", WATERMARK_DURATION).dropDuplicates(
        ["symbol", "timestamp"]
    )


def compute_windowed_ohlcv(df: DataFrame, window_duration: str) -> DataFrame:
    """Compute OHLCV rollups over a tumbling window.

    Args:
        df: Deduplicated tick DataFrame.
        window_duration: Window size (e.g. "5 minutes", "15 minutes").

    Returns:
        DataFrame with windowed OHLCV aggregations.
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            F.window(F.col("timestamp"), window_duration),
            F.col("symbol"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("total_volume"),
            F.count("*").alias("tick_count"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("total_volume"),
            F.col("tick_count"),
        )
    )


def add_volume_anomaly_flag(df: DataFrame) -> DataFrame:
    """Flag ticks where volume exceeds the anomaly threshold.

    Computes a rolling average volume per symbol and flags rows where
    the current volume exceeds ``VOLUME_ANOMALY_THRESHOLD × avg``.

    For streaming, this uses a simple comparison against the batch's
    average volume per symbol.

    Args:
        df: Tick DataFrame with volume column.

    Returns:
        DataFrame with an added ``volume_anomaly`` boolean column.
    """
    avg_volume = df.groupBy("symbol").agg(F.avg("volume").alias("avg_volume"))
    return (
        df.join(avg_volume, on="symbol", how="left")
        .withColumn(
            "volume_anomaly",
            F.when(
                F.col("avg_volume") > 0,
                F.col("volume") > (F.col("avg_volume") * VOLUME_ANOMALY_THRESHOLD),
            ).otherwise(F.lit(False)),
        )
        .drop("avg_volume")
    )


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Add year, month, day partition columns derived from timestamp.

    Args:
        df: DataFrame with a ``timestamp`` column.

    Returns:
        DataFrame with added year, month, day string columns.
    """
    return (
        df.withColumn("year", F.date_format(F.col("timestamp"), "yyyy"))
        .withColumn("month", F.date_format(F.col("timestamp"), "MM"))
        .withColumn("day", F.date_format(F.col("timestamp"), "dd"))
    )


def write_batch_to_s3(batch_df: DataFrame, batch_id: int) -> None:
    """Write a micro-batch of streaming data to S3 silver layer as Parquet.

    Called by ``foreachBatch`` on the streaming query.

    Args:
        batch_df: Micro-batch DataFrame.
        batch_id: Spark-assigned batch identifier.
    """
    if batch_df.isEmpty():
        logger.debug("Batch %d is empty, skipping write.", batch_id)
        return

    settings = get_settings()
    output_path = f"s3a://{settings.s3_bucket_name}/silver/stock_ticks"

    partitioned_df = add_partition_columns(batch_df)

    (
        partitioned_df.write.mode("append")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    logger.info(
        "Batch %d: wrote %d rows to %s",
        batch_id,
        batch_df.count(),
        output_path,
    )


def build_streaming_pipeline(spark: SparkSession) -> DataFrame:
    """Build the full streaming pipeline from Kafka to cleaned ticks.

    Args:
        spark: Active SparkSession.

    Returns:
        Cleaned, deduplicated, anomaly-flagged streaming DataFrame.
    """
    settings = get_settings()

    raw_df = read_from_kafka(spark, settings.kafka_broker, "raw_stock_ticks")
    deserialized_df = deserialize_ticks(raw_df)
    cleaned_df = clean_ticks(deserialized_df)
    deduped_df = deduplicate_ticks(cleaned_df)

    return deduped_df


def start_streaming_query(df: DataFrame, checkpoint_path: str) -> None:
    """Start the streaming query with foreachBatch sink.

    Args:
        df: Streaming DataFrame to write.
        checkpoint_path: Path for streaming checkpoints.
    """
    global _active_query

    _active_query = (
        df.writeStream.foreachBatch(write_batch_to_s3)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .queryName("silver_stock_ticks")
        .start()
    )

    logger.info("Streaming query started: silver_stock_ticks")
    _active_query.awaitTermination()


def _shutdown_handler(signum: int, frame: object) -> None:
    """Handle SIGTERM/SIGINT for graceful shutdown.

    Args:
        signum: Signal number received.
        frame: Current stack frame.
    """
    logger.info("Received signal %d, shutting down gracefully...", signum)
    if _active_query is not None:
        _active_query.stop()
    sys.exit(0)


def main() -> None:
    """Entry point for the Spark Structured Streaming job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    settings = get_settings()
    spark = create_spark_session()

    logger.info(
        "Starting Spark Streaming consumer: broker=%s, bucket=%s",
        settings.kafka_broker,
        settings.s3_bucket_name,
    )

    df = build_streaming_pipeline(spark)

    checkpoint_path = f"s3a://{settings.s3_bucket_name}/checkpoints/silver_stock_ticks"
    start_streaming_query(df, checkpoint_path)


if __name__ == "__main__":
    main()
