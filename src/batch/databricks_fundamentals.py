"""Manual Databricks fundamentals vertical slice using runtime Spark and Delta.

R6's first migrated legacy product (fundamental_enrichment.py): a local
fetcher lands periodic yfinance snapshots to S3 landing/fundamentals/; this
job ingests them via Auto Loader, validates/classifies, and publishes the
latest known snapshot per symbol as gold - structurally identical to
databricks_ticks.py's slice (same bronze/silver/gold/state pattern, same
serverless-only compute constraints), but simpler: fundamentals have no
Kafka transport layer to preserve and no aggregation step, just
validate -> keep-latest-per-symbol -> project.
"""

import argparse
import logging
from typing import Literal

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.landed_fundamentals import (
    BUSINESS_KEYS,
    classify_fundamentals,
    project_fundamentals,
)

logger = logging.getLogger(__name__)
PIPELINE_REVISION = "fundamentals-v1"
Dataset = Literal["raw", "quarantine", "summary", "state"]
TABLES: dict[Dataset, tuple[str, str]] = {
    "raw": ("bronze", "fundamentals_raw"),
    "quarantine": ("silver", "fundamentals_quarantine"),
    "summary": ("gold", "fundamentals"),
    "state": ("gold", "fundamentals_pipeline_state"),
}
STATE_SCHEMA = (
    "status string, bronze_version long, pipeline_revision string, "
    "accepted_rows long, superseded_rows long, quarantined_rows long, "
    "quarantine_version long, summary_version long"
)


class FundamentalsJobConfig(BaseModel):
    """Explicit job parameters with isolated storage and safe UC identifiers.

    Attributes:
        catalog: Existing Unity Catalog catalog.
        schema_prefix: Prefix for existing bronze/silver/gold schemas.
        bucket: Existing S3 bucket, accessed through Unity Catalog.
        max_input_rows: Full-rebuild safety limit for this small portfolio slice.
    """

    catalog: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_prefix: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    max_input_rows: int = Field(default=10000, ge=1, le=1000000)

    def table(self, dataset: Dataset) -> str:
        """Return the qualified identifier for an owned dataset."""
        layer, name = TABLES[dataset]
        return f"{self.catalog}.{self.schema_prefix}_{layer}.{name}"

    def path(self, dataset: Dataset) -> str:
        """Return its isolated external Delta storage location."""
        layer, name = TABLES[dataset]
        return (
            f"s3://{self.bucket}/lakehouse/{layer}/"
            f"{self.catalog}/{self.schema_prefix}/{name}"
        )

    @property
    def checkpoint(self) -> str:
        """Return durable per-target Auto Loader checkpoint storage."""
        return (
            f"s3://{self.bucket}/checkpoints/hybrid/"
            f"{self.catalog}/{self.schema_prefix}/fundamentals_raw"
        )


def validate_locations(spark: SparkSession, config: FundamentalsJobConfig) -> None:
    """Reject existing tables with the wrong format or external location.

    Args:
        spark: Runtime-provided Spark session.
        config: Expected storage and table identities.

    Raises:
        ValueError: If a target name belongs to an unexpected table.
    """
    for dataset in TABLES:
        table = config.table(dataset)
        if spark.catalog.tableExists(table):
            detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
            if (
                detail is None
                or detail["format"] != "delta"
                or detail["location"].rstrip("/") != config.path(dataset)
            ):
                raise ValueError(f"Refusing unexpected table format/location: {table}")


def table_version(spark: SparkSession, table: str) -> int:
    """Read the current Delta version; missing history is a hard failure."""
    history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    if history is None:
        raise ValueError(f"No Delta history for {table}")
    return int(history["version"])


def ingest_raw(spark: SparkSession, config: FundamentalsJobConfig) -> None:
    """Append raw NDJSON lines through terminating Auto Loader ingestion.

    Args:
        spark: Databricks runtime session with Auto Loader support.
        config: Existing storage/catalog configuration.
    """
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", f"{config.checkpoint}/schema")
        .option("cloudFiles.partitionColumns", "")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json.gz")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .schema("value string")
        .load(f"s3://{config.bucket}/landing/fundamentals/")
        .select(
            F.col("value").alias("raw_json"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("_metadata.file_modification_time").alias("file_modified_at"),
            F.current_timestamp().alias("bronze_ingested_at"),
        )
    )
    query = (
        raw.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config.checkpoint}/stream")
        .option("path", config.path("raw"))
        .trigger(availableNow=True)
        .toTable(config.table("raw"))
    )
    try:
        if not query.awaitTermination(900):
            raise TimeoutError("Auto Loader exceeded the 15-minute ingestion limit")
    finally:
        if query.isActive:
            query.stop()


def write_snapshot(
    frame: DataFrame, config: FundamentalsJobConfig, dataset: Dataset
) -> None:
    """Atomically replace one owned Delta dataset, including valid empty results."""
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("path", config.path(dataset))
        .saveAsTable(config.table(dataset))
    )


def snapshot_is_current(
    spark: SparkSession, config: FundamentalsJobConfig, bronze_version: int
) -> bool:
    """Skip recomputation only when a completed version set is still intact."""
    if not spark.catalog.tableExists(config.table("state")):
        return False
    rows = spark.table(config.table("state")).limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Pipeline state must have exactly one control row")
    state = rows[0]
    if (
        state["status"] != "completed"
        or state["bronze_version"] != bronze_version
        or state["pipeline_revision"] != PIPELINE_REVISION
    ):
        return False
    datasets: tuple[Dataset, ...] = ("quarantine", "summary")
    for dataset in datasets:
        if (
            not spark.catalog.tableExists(config.table(dataset))
            or table_version(spark, config.table(dataset))
            != state[f"{dataset}_version"]
        ):
            return False
    return True


def rebuild_outputs(
    spark: SparkSession, config: FundamentalsJobConfig, bronze_version: int
) -> dict[str, int]:
    """Rebuild this small slice from a pinned version and mark completion last.

    Args:
        spark: Runtime session; UTC must be configured by the caller.
        config: Table identities and full-rebuild size guard.
        bronze_version: Exact input Delta version for all derived outputs.

    Returns:
        Counts reconciling accepted, superseded, and quarantined bronze rows.

    Raises:
        ValueError: When the size guard or reconciliation fails.
    """
    state = ["processing", bronze_version, PIPELINE_REVISION, 0, 0, 0, -1, -1]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    bronze = spark.read.option("versionAsOf", bronze_version).table(config.table("raw"))
    input_count = bronze.limit(config.max_input_rows + 1).count()
    if input_count > config.max_input_rows:
        raise ValueError(
            "Bronze exceeds the full-rebuild row limit; incremental design required"
        )
    # No .cache()/.unpersist(): PERSIST TABLE is not supported on serverless
    # compute (Spark Connect). classified is recomputed on each action below;
    # acceptable at this slice's bounded (<= max_input_rows) scale.
    classified = classify_fundamentals(bronze)
    counts = {"accepted": 0, "superseded": 0, "quarantined": 0}
    for row in classified.groupBy("record_status").count().collect():
        counts[row["record_status"]] = row["count"]
    if sum(counts.values()) != input_count:
        raise ValueError("Bronze classification counts do not reconcile")
    accepted = classified.filter(F.col("record_status") == "accepted")
    if accepted.groupBy(*BUSINESS_KEYS).count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate business keys in accepted fundamentals")
    rejected = classified.filter(F.col("record_status") == "quarantined").drop(
        "raw_json", "record_status"
    )
    gold = project_fundamentals(accepted)
    outputs: dict[Dataset, DataFrame] = {
        "quarantine": rejected,
        "summary": gold,
    }
    for dataset, frame in outputs.items():
        write_snapshot(
            frame.withColumn("bronze_version", F.lit(bronze_version)),
            config,
            dataset,
        )
    state = [
        "completed",
        bronze_version,
        PIPELINE_REVISION,
        counts["accepted"],
        counts["superseded"],
        counts["quarantined"],
        *(table_version(spark, config.table(dataset)) for dataset in outputs),
    ]
    write_snapshot(spark.createDataFrame([tuple(state)], STATE_SCHEMA), config, "state")
    logger.info("Completed bronze_version=%s counts=%s", bronze_version, counts)
    return counts


def run(spark: SparkSession, config: FundamentalsJobConfig) -> dict[str, int] | None:
    """Ingest then rebuild pending versions, retaining checkpoint progress on retry."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    validate_locations(spark, config)
    ingest_raw(spark, config)
    version = table_version(spark, config.table("raw"))
    if snapshot_is_current(spark, config, version):
        logger.info("No pending bronze version; skipping transformations")
        return None
    return rebuild_outputs(spark, config, version)


def main() -> None:
    """Run only when explicitly invoked by a Databricks wheel task."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--max-input-rows", type=int, default=10000)
    config = FundamentalsJobConfig(**vars(parser.parse_args()))
    logging.basicConfig(level=logging.INFO)
    run(SparkSession.builder.getOrCreate(), config)


if __name__ == "__main__":
    main()
