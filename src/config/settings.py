"""Application settings loaded from environment variables."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration for the stock market pipeline.

    All values are read from environment variables. Defaults are provided
    for non-sensitive settings. Sensitive values (API keys, AWS credentials)
    must be set explicitly.

    Attributes:
        alpha_vantage_api_key: API key for Alpha Vantage data provider.
        aws_access_key_id: AWS access key for S3/Glue/Athena access.
        aws_secret_access_key: AWS secret key.
        aws_session_token: Optional token for temporary AWS credentials.
        aws_default_region: AWS region for all services.
        s3_bucket_name: S3 bucket used as the data lake.
        kafka_broker: Kafka broker connection string.
        run_pipeline: Kill switch to enable/disable the Kafka producer.
        max_iterations: Auto-stop after N polling cycles (0 = unlimited).
        poll_interval_seconds: Seconds between producer polling cycles.
        environment: Deployment environment (dev, staging, prod).
        raw_landing_enabled: Explicit opt-in to raw Kafka landing.
        raw_source_id: Stable Kafka source incarnation; required when enabled.
        raw_consumer_group: Dedicated group, separate from silver processing.
        raw_topic: Kafka topic containing original source messages.
        raw_spool_path: Durable local SQLite pending-record database.
        raw_flush_interval_seconds: Maximum normal batch buffering interval.
        raw_max_records: Poll and upload record limit.
        raw_max_batch_bytes: Uncompressed NDJSON limit per upload.
    """

    alpha_vantage_api_key: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_session_token: str = ""
    aws_default_region: str = "us-east-1"
    s3_bucket_name: str = "stock-market-datalake"
    kafka_broker: str = "localhost:9092"
    run_pipeline: bool = True
    max_iterations: int = 0
    poll_interval_seconds: int = 60
    environment: str = "dev"

    raw_landing_enabled: bool = False
    raw_source_id: str = Field(
        default="", pattern=r"^([A-Za-z0-9][A-Za-z0-9_.-]{0,63})?$"
    )
    raw_consumer_group: str = Field(default="stock-raw-landing-v1", min_length=1)
    raw_topic: str = Field(
        default="raw_stock_ticks", pattern=r"^[A-Za-z0-9_-][A-Za-z0-9_.-]*$"
    )
    raw_spool_path: Path = Path(".state/raw-landing.sqlite3")
    raw_flush_interval_seconds: int = Field(default=60, ge=1, le=120)
    raw_max_records: int = Field(default=500, ge=1, le=10000)
    raw_max_batch_bytes: int = Field(
        default=5 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024
    )

    # extra="ignore": the shared repo-root .env also carries credentials for
    # other consumers that don't go through this Settings model (e.g. the
    # dashboard's Snowflake connection, read directly from os.environ) -
    # this model should validate its own known fields, not reject the file
    # over keys it was never meant to own.
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


def get_settings() -> Settings:
    """Create and return a Settings instance.

    Returns:
        Settings: Populated configuration object.
    """
    return Settings()
