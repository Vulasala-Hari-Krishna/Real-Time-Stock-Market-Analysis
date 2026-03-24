"""Application settings loaded from environment variables."""

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
        aws_default_region: AWS region for all services.
        s3_bucket_name: S3 bucket used as the data lake.
        kafka_broker: Kafka broker connection string.
        run_pipeline: Kill switch to enable/disable the Kafka producer.
        max_iterations: Auto-stop after N polling cycles (0 = unlimited).
        poll_interval_seconds: Seconds between producer polling cycles.
        environment: Deployment environment (dev, staging, prod).
    """

    alpha_vantage_api_key: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_default_region: str = "us-east-1"
    s3_bucket_name: str = "stock-market-datalake"
    kafka_broker: str = "localhost:9092"
    run_pipeline: bool = True
    max_iterations: int = 0
    poll_interval_seconds: int = 60
    environment: str = "dev"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def get_settings() -> Settings:
    """Create and return a Settings instance.

    Returns:
        Settings: Populated configuration object.
    """
    return Settings()
