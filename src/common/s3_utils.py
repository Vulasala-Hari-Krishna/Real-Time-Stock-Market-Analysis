"""S3 utility functions for reading and writing data to the data lake.

Provides helpers to upload JSON and Parquet data to S3 and generate
partitioned S3 keys following the medallion architecture.
"""

import io
import json
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_client(region: str = "us-east-1") -> boto3.client:
    """Create and return a boto3 S3 client.

    Args:
        region: AWS region name.

    Returns:
        A boto3 S3 client.
    """
    return boto3.client("s3", region_name=region)


def upload_json_to_s3(
    data: dict | list,
    bucket: str,
    key: str,
    region: str = "us-east-1",
) -> bool:
    """Upload a JSON-serializable object to S3.

    Args:
        data: Python dict or list to serialize as JSON.
        bucket: Target S3 bucket name.
        key: S3 object key (path).
        region: AWS region name.

    Returns:
        True if upload succeeded, False otherwise.
    """
    try:
        client = get_s3_client(region)
        body = json.dumps(data, default=str).encode("utf-8")
        client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
        logger.info("Uploaded JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))
        return True
    except ClientError as exc:
        logger.error("Failed to upload JSON to s3://%s/%s: %s", bucket, key, exc)
        return False


def upload_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    region: str = "us-east-1",
) -> bool:
    """Upload a pandas DataFrame as Parquet to S3.

    Args:
        df: DataFrame to write.
        bucket: Target S3 bucket name.
        key: S3 object key (path), should end in .parquet.
        region: AWS region name.

    Returns:
        True if upload succeeded, False otherwise.
    """
    try:
        client = get_s3_client(region)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info("Uploaded Parquet to s3://%s/%s (%d rows)", bucket, key, len(df))
        return True
    except ClientError as exc:
        logger.error("Failed to upload Parquet to s3://%s/%s: %s", bucket, key, exc)
        return False


def read_parquet_from_s3(
    bucket: str,
    key: str,
    region: str = "us-east-1",
) -> pd.DataFrame | None:
    """Read a Parquet file from S3 into a pandas DataFrame.

    Args:
        bucket: S3 bucket name.
        key: S3 object key (path).
        region: AWS region name.

    Returns:
        DataFrame with the data, or None on failure.
    """
    try:
        client = get_s3_client(region)
        response = client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(buffer, engine="pyarrow")
        logger.info("Read Parquet from s3://%s/%s (%d rows)", bucket, key, len(df))
        return df
    except ClientError as exc:
        logger.error("Failed to read Parquet from s3://%s/%s: %s", bucket, key, exc)
        return None


def generate_s3_key(
    layer: str,
    table: str,
    partition_date: datetime | None = None,
    symbol: str | None = None,
) -> str:
    """Generate a partitioned S3 key following the medallion architecture.

    Produces keys like:
    ``bronze/stock_ticks/year=2024/month=03/day=15/AAPL_20240315T143000Z.json``

    Args:
        layer: Data lake layer (bronze, silver, gold).
        table: Table or dataset name.
        partition_date: Date for partitioning (defaults to UTC now).
        symbol: Optional ticker symbol to include in the filename.

    Returns:
        Generated S3 key string.
    """
    if partition_date is None:
        partition_date = datetime.now(tz=timezone.utc)

    year = partition_date.strftime("%Y")
    month = partition_date.strftime("%m")
    day = partition_date.strftime("%d")
    timestamp_str = partition_date.strftime("%Y%m%dT%H%M%SZ")

    parts = [
        layer,
        table,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    ]

    extension = "json" if layer == "bronze" else "parquet"
    filename = (
        f"{symbol}_{timestamp_str}.{extension}"
        if symbol
        else f"{timestamp_str}.{extension}"
    )
    parts.append(filename)

    return "/".join(parts)
