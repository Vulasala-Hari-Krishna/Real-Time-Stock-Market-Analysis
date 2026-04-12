"""Data quality checks DAG.

Runs after the daily batch aggregation to verify data freshness,
completeness, null values, and schema conformity.  All checks run
in parallel; any failure triggers an alert.

These checks use boto3 and pyarrow to read S3 data directly —
no Spark session is needed for lightweight quality validation.

Schedule: weekdays at 08:00 UTC.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def _get_s3_client():
    """Create an authenticated S3 client and return (client, bucket)."""
    import boto3

    from src.config.settings import get_settings

    settings = get_settings()
    client = boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )
    return client, settings.s3_bucket_name


def _read_parquet_sample(s3, bucket: str, prefix: str):
    """Read a single Parquet file from S3 as a pyarrow Table."""
    import pyarrow.parquet as pq

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20)
    keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not keys:
        raise ValueError(f"No parquet files found under {prefix}")

    obj = s3.get_object(Bucket=bucket, Key=keys[0])
    return pq.read_table(io.BytesIO(obj["Body"].read()))


@dag(
    dag_id="data_quality_checks",
    description="Validate freshness, completeness, nulls, and schema of pipeline data",
    schedule="0 8 * * 1-5",
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
        """Verify the latest silver-layer data is recent.

        Navigates Hive-style partitions (symbol/year/month) to find
        the most recent data and checks it is within the last 4 days.

        Raises:
            ValueError: If data is stale.
        """
        from src.config.watchlist import SYMBOLS

        s3, bucket = _get_s3_client()
        symbol = SYMBOLS[0]
        base = f"silver/historical/symbol={symbol}/"

        # Navigate to the latest year partition
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=base, Delimiter="/")
        year_prefixes = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
        if not year_prefixes:
            raise ValueError(f"No year partitions found under {base}")

        # Navigate to the latest month within the latest year
        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=year_prefixes[-1],
            Delimiter="/",
        )
        month_prefixes = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
        if not month_prefixes:
            raise ValueError(f"No month partitions found under {year_prefixes[-1]}")

        table = _read_parquet_sample(s3, bucket, month_prefixes[-1])
        dates = table.column("date").to_pylist()
        max_date = max(dates)
        if hasattr(max_date, "date"):
            max_date = max_date.date()

        threshold = (datetime.utcnow() - timedelta(days=4)).date()
        if max_date < threshold:
            raise ValueError(
                f"Data is stale: latest date={max_date}, "
                f"expected at least {threshold}"
            )
        logger.info("Freshness OK: latest date=%s", max_date)

    @task()
    def check_completeness() -> None:
        """Verify all watchlist symbols have data in the silver layer.

        Lists S3 partition prefixes and compares against the watchlist.

        Raises:
            ValueError: If any symbol is missing.
        """
        from src.config.watchlist import SYMBOLS

        s3, bucket = _get_s3_client()

        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix="silver/historical/symbol=",
            Delimiter="/",
        )
        prefixes = {
            p["Prefix"].split("symbol=")[1].rstrip("/")
            for p in response.get("CommonPrefixes", [])
        }
        missing = set(SYMBOLS) - prefixes
        if missing:
            raise ValueError(f"Missing symbols in silver layer: {sorted(missing)}")
        logger.info("Completeness OK: all %d symbols present", len(SYMBOLS))

    @task()
    def check_null_values() -> None:
        """Sample check for null prices or volumes in the silver layer.

        Reads a sample Parquet file and checks critical columns.

        Raises:
            ValueError: If null values are found.
        """
        s3, bucket = _get_s3_client()
        table = _read_parquet_sample(s3, bucket, "silver/historical/")
        df = table.to_pandas()

        critical_cols = ["open", "high", "low", "close", "volume"]
        existing = [c for c in critical_cols if c in df.columns]
        null_counts = {
            col: int(cnt) for col, cnt in df[existing].isnull().sum().items() if cnt > 0
        }
        if null_counts:
            raise ValueError(f"Null values found in silver layer: {null_counts}")
        logger.info("Null check OK: no nulls in critical columns")

    @task()
    def check_schema() -> None:
        """Verify silver-layer Parquet schema matches expected fields.

        Reads metadata from a sample file and compares field names.

        Raises:
            ValueError: If required fields are missing.
        """
        s3, bucket = _get_s3_client()
        table = _read_parquet_sample(s3, bucket, "silver/historical/")

        actual_fields = set(table.schema.names)
        # 'symbol', 'year', 'month' are Hive partition columns stored
        # in the directory path, not inside individual Parquet files.
        required_fields = {
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
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

    # All quality checks run in parallel
    check_data_freshness()
    check_completeness()
    check_null_values()
    check_schema()


data_quality_checks()
