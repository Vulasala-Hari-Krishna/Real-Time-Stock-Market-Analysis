"""S3 utility functions for reading and writing data to the data lake."""

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_client(region: str | None = None) -> boto3.client:
    """Create and return a boto3 S3 client.

    Args:
        region: AWS region name. Defaults to AWS_DEFAULT_REGION env var.

    Returns:
        A boto3 S3 client.
    """
    if region is None:
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def upload_json_to_s3(
    data: dict | list,
    bucket: str,
    key: str,
    region: str | None = None,
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
