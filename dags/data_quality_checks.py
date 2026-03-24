"""Data quality checks DAG.

Runs after the daily batch aggregation to verify data freshness,
completeness, null values, and schema conformity.  All checks run
in parallel; any failure triggers an alert.

Schedule: daily at 08:00 UTC.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="data_quality_checks",
    description="Validate freshness, completeness, nulls, and schema of pipeline data",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["quality", "monitoring"],
)
def data_quality_checks() -> None:
    """Run all data quality checks in parallel."""

    @task()
    def check_data_freshness() -> None:
        """Verify the latest silver-layer data is from yesterday.

        Raises:
            ValueError: If the most recent date is older than yesterday.
        """
        from src.batch.daily_aggregation import create_spark_session
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session(app_name="DQ_Freshness")
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"

            df = spark.read.parquet(silver_path)
            from pyspark.sql import functions as F

            max_date_row = df.agg(F.max("date").alias("max_date")).collect()
            if not max_date_row or max_date_row[0]["max_date"] is None:
                raise ValueError("No data found in silver layer")

            max_date = max_date_row[0]["max_date"].date()
            yesterday = (datetime.utcnow() - timedelta(days=1)).date()

            # Allow weekends / holidays — data should be within 3 days
            threshold = (datetime.utcnow() - timedelta(days=4)).date()
            if max_date < threshold:
                raise ValueError(
                    f"Data is stale: latest date={max_date}, "
                    f"expected at least {threshold}"
                )
            logger.info(
                "Freshness OK: latest date=%s, yesterday=%s",
                max_date,
                yesterday,
            )
        finally:
            spark.stop()

    @task()
    def check_completeness() -> None:
        """Verify all watchlist symbols have data in the silver layer.

        Raises:
            ValueError: If any symbol is missing from the data.
        """
        from src.batch.daily_aggregation import create_spark_session
        from src.config.settings import get_settings
        from src.config.watchlist import SYMBOLS

        settings = get_settings()
        spark = create_spark_session(app_name="DQ_Completeness")
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"

            df = spark.read.parquet(silver_path)
            present_symbols = {
                row["symbol"]
                for row in df.select("symbol").distinct().collect()
            }
            missing = set(SYMBOLS) - present_symbols
            if missing:
                raise ValueError(
                    f"Missing symbols in silver layer: {sorted(missing)}"
                )
            logger.info(
                "Completeness OK: all %d symbols present", len(SYMBOLS)
            )
        finally:
            spark.stop()

    @task()
    def check_null_values() -> None:
        """Verify no null prices or volumes in the silver layer.

        Raises:
            ValueError: If null prices or volumes are found.
        """
        from src.batch.daily_aggregation import create_spark_session
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session(app_name="DQ_Nulls")
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"

            df = spark.read.parquet(silver_path)
            from pyspark.sql import functions as F

            critical_cols = ["open", "high", "low", "close", "volume"]
            null_counts: dict[str, int] = {}
            for col_name in critical_cols:
                cnt = df.filter(F.col(col_name).isNull()).count()
                if cnt > 0:
                    null_counts[col_name] = cnt

            if null_counts:
                raise ValueError(
                    f"Null values found in silver layer: {null_counts}"
                )
            logger.info("Null check OK: no nulls in critical columns")
        finally:
            spark.stop()

    @task()
    def check_schema() -> None:
        """Verify silver-layer Parquet schema matches expected fields.

        Raises:
            ValueError: If required fields are missing.
        """
        from src.batch.daily_aggregation import create_spark_session
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session(app_name="DQ_Schema")
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"

            df = spark.read.parquet(silver_path)
            actual_fields = {f.name for f in df.schema.fields}
            required_fields = {
                "symbol", "date", "open", "high", "low", "close", "volume",
            }
            missing_fields = required_fields - actual_fields
            if missing_fields:
                raise ValueError(
                    f"Schema mismatch: missing fields {sorted(missing_fields)}"
                )
            logger.info(
                "Schema OK: all required fields present (%s)",
                sorted(required_fields),
            )
        finally:
            spark.stop()

    # All quality checks run in parallel
    check_data_freshness()
    check_completeness()
    check_null_values()
    check_schema()


data_quality_checks()
