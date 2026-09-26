"""Cloud-free runner boundary tests; Spark semantics are checked separately."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.batch import databricks_historical as job


@pytest.fixture()
def config() -> job.HistoricalJobConfig:
    return job.HistoricalJobConfig(
        catalog="portfolio", schema_prefix="stocks", bucket="test-bucket"
    )


def test_config_is_explicit_and_paths_are_isolated(
    config: job.HistoricalJobConfig,
) -> None:
    assert config.table("raw") == "portfolio.stocks_bronze.historical_ohlcv_raw"
    assert (
        config.path("raw")
        == "s3://test-bucket/lakehouse/bronze/portfolio/stocks/historical_ohlcv_raw"
    )
    assert config.table("summary") == "portfolio.stocks_gold.historical_ohlcv"
    assert config.backfill_years == 5


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog", "bad;drop"),
        ("schema_prefix", "x.y"),
        ("bucket", "bucket/path"),
        ("backfill_years", 0),
        ("max_input_rows", 0),
    ],
)
def test_invalid_job_parameters(
    config: job.HistoricalJobConfig, field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        job.HistoricalJobConfig(**{**config.model_dump(), field: value})


@pytest.mark.parametrize(
    "mode", ["missing", "valid", "wrong_format", "wrong_path", "no_detail"]
)
def test_registered_location_guard(config: job.HistoricalJobConfig, mode: str) -> None:
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
        job.table_version(spark, "portfolio.stocks_bronze.historical_ohlcv_raw")
    spark.sql.return_value.first.return_value = {"version": 4}
    assert job.table_version(spark, "portfolio.stocks_bronze.historical_ohlcv_raw") == 4


def test_write_snapshot_does_not_evolve_schema(config: job.HistoricalJobConfig) -> None:
    frame = MagicMock()
    writer = frame.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.options.return_value = writer
    job.write_snapshot(frame, config, "summary")
    writer.mode.assert_called_once_with("overwrite")
    writer.option.assert_called_once_with("path", config.path("summary"))
    writer.options.assert_called_once_with(**job.DELTA_AUTO_OPTIMIZE_PROPERTIES)
    writer.saveAsTable.assert_called_once_with(config.table("summary"))


def test_append_bronze_appends_and_returns_new_version(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    frame = spark.createDataFrame.return_value
    frame.withColumn.return_value = frame
    writer = frame.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.options.return_value = writer
    with patch.object(job, "F"), patch.object(job, "table_version", return_value=7):
        version = job.append_bronze(spark, config, [{"symbol": "AAPL"}])

    assert version == 7
    writer.mode.assert_called_once_with("append")
    writer.saveAsTable.assert_called_once_with(config.table("raw"))


# ---------------------------------------------------------------------------
# rebuild_outputs
# ---------------------------------------------------------------------------
def test_rebuild_outputs_refuses_an_empty_fetch(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    with patch.object(job, "fetch_all_history", return_value=[]), patch.object(
        job, "append_bronze"
    ) as append:
        with pytest.raises(RuntimeError, match="refusing to append an empty"):
            job.rebuild_outputs(spark, config, ["AAPL"])
    append.assert_not_called()
    first_state = spark.createDataFrame.call_args_list[0].args[0][0]
    assert first_state[0] == "processing"


def test_rebuild_outputs_enforces_the_full_rebuild_size_guard(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = config.max_input_rows + 1
    with patch.object(
        job, "fetch_all_history", return_value=[{"symbol": "AAPL"}]
    ), patch.object(job, "append_bronze", return_value=3):
        with pytest.raises(ValueError, match="full-rebuild row limit"):
            job.rebuild_outputs(spark, config, ["AAPL"])
    spark.read.option.assert_called_with("versionAsOf", 3)


def test_rebuild_outputs_publishes_gold_and_completes_state(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = 2
    latest = MagicMock()
    gold = MagicMock()
    gold.withColumn.return_value = gold
    gold.count.return_value = 2
    rows = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
    with patch.object(job, "fetch_all_history", return_value=rows), patch.object(
        job, "append_bronze", return_value=5
    ), patch.object(
        job, "rank_latest_per_symbol_date", return_value=latest
    ), patch.object(
        job, "project_historical", return_value=gold
    ), patch.object(
        job, "write_snapshot"
    ) as write, patch.object(
        job, "table_version", return_value=9
    ), patch.object(
        job, "F"
    ):
        counts = job.rebuild_outputs(spark, config, ["AAPL", "MSFT"])

    assert counts == {"fetched": 2, "skipped_symbols": 0, "gold_rows": 2}
    assert [call.args[2] for call in write.call_args_list] == [
        "state",
        "summary",
        "state",
    ]
    states = [call.args[0][0][0] for call in spark.createDataFrame.call_args_list]
    assert states == ["processing", "completed"]


def test_rebuild_outputs_counts_skipped_symbols(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = 1
    gold = MagicMock()
    gold.withColumn.return_value = gold
    gold.count.return_value = 1
    with patch.object(
        job, "fetch_all_history", return_value=[{"symbol": "AAPL"}]
    ), patch.object(job, "append_bronze", return_value=5), patch.object(
        job, "rank_latest_per_symbol_date"
    ), patch.object(
        job, "project_historical", return_value=gold
    ), patch.object(
        job, "write_snapshot"
    ), patch.object(
        job, "table_version", return_value=9
    ), patch.object(
        job, "F"
    ):
        counts = job.rebuild_outputs(spark, config, ["AAPL", "MSFT", "GOOGL"])

    assert counts == {"fetched": 1, "skipped_symbols": 2, "gold_rows": 1}


# ---------------------------------------------------------------------------
# run / main
# ---------------------------------------------------------------------------
def test_run_always_rebuilds_no_idempotency_skip(
    config: job.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    with patch.object(job, "validate_locations") as validate, patch.object(
        job, "rebuild_outputs", return_value={"fetched": 3}
    ) as rebuild:
        result = job.run(spark, config, symbols=["AAPL"])
    validate.assert_called_once_with(spark, config)
    rebuild.assert_called_once_with(spark, config, ["AAPL"])
    assert result == {"fetched": 3}
    spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")


def test_entrypoint_passes_explicit_job_parameters() -> None:
    with patch(
        "sys.argv",
        [
            "historical",
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


def test_bundle_is_manual_bounded_and_uses_explicit_platform_inputs() -> None:
    from pathlib import Path

    import tomllib
    import yaml

    root = Path(__file__).resolve().parents[2]
    bundle = yaml.safe_load((root / "databricks/databricks.yml").read_text())
    deployed = bundle["resources"]["jobs"]["landed_historical"]
    assert not {"schedule", "trigger", "continuous"}.intersection(deployed)
    assert deployed["max_concurrent_runs"] == 1
    assert deployed["queue"]["enabled"] is False
    task = deployed["tasks"][0]
    assert task["max_retries"] == 0
    assert task["python_wheel_task"]["entry_point"] == "landed_historical"
    assert "job_clusters" not in deployed
    environment_key = task["environment_key"]
    environments = {env["environment_key"]: env for env in deployed["environments"]}
    dependencies = environments[environment_key]["spec"]["dependencies"]
    assert any("yfinance" in dep for dep in dependencies)
    assert "run_as" not in bundle
    package = tomllib.loads((root / "databricks/pyproject.toml").read_text())
    assert (
        package["project"]["scripts"]["landed_historical"]
        == "src.batch.databricks_historical:main"
    )
