"""Cloud-free runner boundary tests; Spark semantics are checked separately."""

import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from src.batch import databricks_fundamentals as job
from src.batch import landed_fundamentals


@pytest.fixture()
def config() -> job.FundamentalsJobConfig:
    return job.FundamentalsJobConfig(
        catalog="portfolio", schema_prefix="stocks", bucket="test-bucket"
    )


def test_config_is_explicit_and_paths_are_isolated(
    config: job.FundamentalsJobConfig,
) -> None:
    assert config.table("raw") == "portfolio.stocks_bronze.fundamentals_raw"
    assert (
        config.path("raw")
        == "s3://test-bucket/lakehouse/bronze/portfolio/stocks/fundamentals_raw"
    )
    assert (
        config.checkpoint
        == "s3://test-bucket/checkpoints/hybrid/portfolio/stocks/fundamentals_raw"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog", "bad;drop"),
        ("schema_prefix", "x.y"),
        ("bucket", "bucket/path"),
        ("max_input_rows", 0),
    ],
)
def test_invalid_job_parameters(
    config: job.FundamentalsJobConfig, field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        job.FundamentalsJobConfig(**{**config.model_dump(), field: value})


@pytest.mark.parametrize(
    "mode", ["missing", "valid", "wrong_format", "wrong_path", "no_detail"]
)
def test_registered_location_guard(
    config: job.FundamentalsJobConfig, mode: str
) -> None:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = mode != "missing"
    details = [
        {"format": "delta", "location": config.path(dataset) + "/"}
        for dataset in job.TABLES
    ]
    if mode == "wrong_format":
        details[0]["format"] = "parquet"
    if mode == "wrong_path":
        details[0]["location"] = "s3://legacy/gold/"
    if mode == "no_detail":
        details[0] = None
    spark.sql.return_value.first.side_effect = details
    if mode in {"wrong_format", "wrong_path", "no_detail"}:
        with pytest.raises(ValueError, match="Refusing unexpected"):
            job.validate_locations(spark, config)
    else:
        job.validate_locations(spark, config)


def test_missing_history_fails() -> None:
    spark = MagicMock()
    spark.sql.return_value.first.return_value = None
    with pytest.raises(ValueError, match="No Delta history"):
        job.table_version(spark, "portfolio.stocks_bronze.fundamentals_raw")
    spark.sql.return_value.first.return_value = {"version": 4}
    assert job.table_version(spark, "portfolio.stocks_bronze.fundamentals_raw") == 4


@pytest.mark.parametrize("finishes", [True, False])
def test_available_now_and_timeout(
    config: job.FundamentalsJobConfig, finishes: bool
) -> None:
    spark = MagicMock()
    reader = spark.readStream.format.return_value
    reader.option.return_value = reader
    reader.schema.return_value = reader
    frame = reader.load.return_value.select.return_value
    writer = frame.writeStream.format.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    query = writer.toTable.return_value
    query.awaitTermination.return_value = finishes
    query.isActive = not finishes
    with patch.object(job, "F"):
        if finishes:
            job.ingest_raw(spark, config)
            query.stop.assert_not_called()
        else:
            with pytest.raises(TimeoutError):
                job.ingest_raw(spark, config)
            query.stop.assert_called_once()
    spark.readStream.format.assert_called_once_with("cloudFiles")
    reader.option.assert_any_call("cloudFiles.format", "text")
    reader.load.assert_called_once_with("s3://test-bucket/landing/fundamentals/")
    writer.trigger.assert_called_once_with(availableNow=True)
    writer.option.assert_any_call("checkpointLocation", config.checkpoint + "/stream")


def test_snapshot_write_does_not_evolve_schema(
    config: job.FundamentalsJobConfig,
) -> None:
    frame = MagicMock()
    writer = frame.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    job.write_snapshot(frame, config, "summary")
    writer.mode.assert_called_once_with("overwrite")
    writer.option.assert_called_once_with("path", config.path("summary"))
    writer.saveAsTable.assert_called_once_with(config.table("summary"))


@pytest.mark.parametrize(
    "mode",
    [
        "absent",
        "empty",
        "multiple",
        "processing",
        "stale_input",
        "revision",
        "missing_output",
        "changed_output",
        "current",
    ],
)
def test_noop_requires_intact_completed_versions(
    config: job.FundamentalsJobConfig, mode: str
) -> None:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = mode != "absent"
    state = {
        "status": "completed",
        "bronze_version": 2,
        "pipeline_revision": job.PIPELINE_REVISION,
        "quarantine_version": 4,
        "summary_version": 4,
    }
    if mode == "processing":
        state["status"] = "processing"
    if mode == "stale_input":
        state["bronze_version"] = 1
    if mode == "revision":
        state["pipeline_revision"] = "old"
    if mode == "missing_output":
        spark.catalog.tableExists.side_effect = [True, False]
    spark.table.return_value.limit.return_value.collect.return_value = (
        [] if mode == "empty" else [state, state] if mode == "multiple" else [state]
    )
    with patch.object(
        job, "table_version", return_value=5 if mode == "changed_output" else 4
    ):
        if mode in {"empty", "multiple"}:
            with pytest.raises(ValueError, match="exactly one"):
                job.snapshot_is_current(spark, config, 2)
        else:
            assert job.snapshot_is_current(spark, config, 2) is (mode == "current")


@pytest.mark.parametrize("failure", [None, "row_limit", "counts", "duplicate", "write"])
def test_publication_state_is_completed_only_after_validated_outputs(
    config: job.FundamentalsJobConfig, failure: str | None
) -> None:
    spark = MagicMock()
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = (
        config.max_input_rows + 1 if failure == "row_limit" else 3
    )
    classified = MagicMock()
    classified.groupBy.return_value.count.return_value.collect.return_value = [
        {"record_status": "accepted", "count": 1},
        {"record_status": "superseded", "count": 1},
        {"record_status": "quarantined", "count": 0 if failure == "counts" else 1},
    ]
    accepted = classified.filter.return_value
    accepted.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.count.return_value = (
        1 if failure == "duplicate" else 0
    )
    gold = MagicMock()
    with patch.object(
        job, "classify_fundamentals", return_value=classified
    ), patch.object(job, "project_fundamentals", return_value=gold), patch.object(
        job, "write_snapshot"
    ) as write, patch.object(
        job, "table_version", return_value=5
    ), patch.object(
        job, "F"
    ):
        if failure == "write":
            write.side_effect = [None, None, RuntimeError("write failed")]
        if failure:
            with pytest.raises((ValueError, RuntimeError)):
                job.rebuild_outputs(spark, config, 2)
        else:
            assert job.rebuild_outputs(spark, config, 2) == {
                "accepted": 1,
                "superseded": 1,
                "quarantined": 1,
            }
            assert [call.args[2] for call in write.call_args_list] == [
                "state",
                "quarantine",
                "summary",
                "state",
            ]
        states = [call.args[0][0][0] for call in spark.createDataFrame.call_args_list]
        assert states == (["processing"] if failure else ["processing", "completed"])
    spark.read.option.assert_called_once_with("versionAsOf", 2)


@pytest.mark.parametrize("current", [True, False])
def test_retry_after_ingestion_recovers_unfinished_outputs(
    config: job.FundamentalsJobConfig, current: bool
) -> None:
    spark = MagicMock()
    with patch.object(job, "validate_locations") as validate, patch.object(
        job, "ingest_raw"
    ) as ingest, patch.object(job, "table_version", return_value=4), patch.object(
        job, "snapshot_is_current", return_value=current
    ), patch.object(
        job, "rebuild_outputs", return_value={"accepted": 3}
    ) as rebuild:
        result = job.run(spark, config)
        validate.assert_called_once_with(spark, config)
        ingest.assert_called_once_with(spark, config)
        if current:
            assert result is None
            rebuild.assert_not_called()
        else:
            assert result == {"accepted": 3}
            rebuild.assert_called_once_with(spark, config, 4)
    spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")


def test_entrypoint_passes_explicit_job_parameters() -> None:
    with patch(
        "sys.argv",
        [
            "fundamentals",
            "--catalog",
            "portfolio",
            "--schema-prefix",
            "stocks",
            "--bucket",
            "test-bucket",
        ],
    ), patch.object(job, "SparkSession"), patch.object(job, "run") as run:
        job.main()
    assert run.call_args.args[1].catalog == "portfolio"


def test_spark_expression_builders_require_no_platform_clients() -> None:
    frame = MagicMock()
    with patch.object(landed_fundamentals, "F") as functions, patch.object(
        landed_fundamentals, "Window"
    ):
        functions.col.return_value.__gt__.return_value = MagicMock()
        landed_fundamentals.classify_fundamentals(frame)
        functions.udf.assert_called_once_with(
            landed_fundamentals.normalize_fundamentals_record,
            landed_fundamentals.NORMALIZED_SCHEMA,
        )


def test_bundle_is_manual_bounded_and_uses_explicit_platform_inputs() -> None:
    root = Path(__file__).resolve().parents[2]
    bundle = yaml.safe_load((root / "databricks/databricks.yml").read_text())
    deployed = bundle["resources"]["jobs"]["landed_fundamentals"]
    assert not {"schedule", "trigger", "continuous"}.intersection(deployed)
    assert deployed["max_concurrent_runs"] == 1
    assert deployed["timeout_seconds"] == 1800
    assert deployed["queue"]["enabled"] is False
    task = deployed["tasks"][0]
    assert task["max_retries"] == 0
    assert task["retry_on_timeout"] is False
    assert task["python_wheel_task"]["entry_point"] == "landed_fundamentals"
    assert "job_clusters" not in deployed
    environment_key = task["environment_key"]
    environments = {env["environment_key"]: env for env in deployed["environments"]}
    assert environment_key in environments
    assert environments[environment_key]["spec"]["dependencies"]
    assert "run_as" not in bundle
    package = tomllib.loads((root / "databricks/pyproject.toml").read_text())
    assert (
        package["project"]["scripts"]["landed_fundamentals"]
        == "src.batch.databricks_fundamentals:main"
    )
