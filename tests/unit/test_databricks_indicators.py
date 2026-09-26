"""Cloud-free runner boundary tests; Spark semantics are checked separately
(the indicator/signal/correlation logic itself is daily_aggregation.py's,
reused directly and already covered by test_daily_aggregation.py)."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.batch import databricks_indicators as job


@pytest.fixture()
def config() -> job.IndicatorsJobConfig:
    return job.IndicatorsJobConfig(
        catalog="portfolio", schema_prefix="stocks", bucket="test-bucket"
    )


def test_config_is_explicit_and_paths_are_isolated(
    config: job.IndicatorsJobConfig,
) -> None:
    assert config.table("daily_summaries") == "portfolio.stocks_gold.daily_summaries"
    assert (
        config.path("daily_summaries")
        == "s3://test-bucket/lakehouse/gold/portfolio/stocks/daily_summaries"
    )
    assert config.source_table() == "portfolio.stocks_gold.historical_ohlcv"


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
    config: job.IndicatorsJobConfig, field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        job.IndicatorsJobConfig(**{**config.model_dump(), field: value})


@pytest.mark.parametrize(
    "mode", ["missing", "valid", "wrong_format", "wrong_path", "no_detail"]
)
def test_registered_location_guard(config: job.IndicatorsJobConfig, mode: str) -> None:
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
        job.table_version(spark, "portfolio.stocks_gold.historical_ohlcv")
    spark.sql.return_value.first.return_value = {"version": 4}
    assert job.table_version(spark, "portfolio.stocks_gold.historical_ohlcv") == 4


def test_write_snapshot_does_not_evolve_schema(config: job.IndicatorsJobConfig) -> None:
    frame = MagicMock()
    writer = frame.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.options.return_value = writer
    job.write_snapshot(frame, config, "daily_summaries")
    writer.mode.assert_called_once_with("overwrite")
    writer.option.assert_called_once_with("path", config.path("daily_summaries"))
    writer.options.assert_called_once_with(**job.DELTA_AUTO_OPTIMIZE_PROPERTIES)
    writer.saveAsTable.assert_called_once_with(config.table("daily_summaries"))


def test_read_source_pins_version_and_selects_ohlcv_columns(
    config: job.IndicatorsJobConfig,
) -> None:
    spark = MagicMock()
    reader = spark.read.option.return_value
    selected = reader.table.return_value.select.return_value
    with patch.object(job, "table_version", return_value=6):
        df, version = job.read_source(spark, config)

    assert version == 6
    spark.read.option.assert_called_once_with("versionAsOf", 6)
    reader.table.assert_called_once_with(config.source_table())
    reader.table.return_value.select.assert_called_once_with(
        "symbol", "date", "open", "high", "low", "close", "volume"
    )
    assert df is selected


# ---------------------------------------------------------------------------
# rebuild_outputs
# ---------------------------------------------------------------------------
def test_rebuild_outputs_refuses_an_empty_source(
    config: job.IndicatorsJobConfig,
) -> None:
    spark = MagicMock()
    source_df = MagicMock()
    source_df.limit.return_value.count.return_value = 0
    with patch.object(job, "read_source", return_value=(source_df, 3)):
        with pytest.raises(RuntimeError, match="is empty"):
            job.rebuild_outputs(spark, config)
    first_state = spark.createDataFrame.call_args_list[0].args[0][0]
    assert first_state[0] == "processing"


def test_rebuild_outputs_enforces_the_full_rebuild_size_guard(
    config: job.IndicatorsJobConfig,
) -> None:
    spark = MagicMock()
    source_df = MagicMock()
    source_df.limit.return_value.count.return_value = config.max_input_rows + 1
    with patch.object(job, "read_source", return_value=(source_df, 3)):
        with pytest.raises(ValueError, match="full-rebuild row limit"):
            job.rebuild_outputs(spark, config)


def test_rebuild_outputs_publishes_all_three_gold_tables_and_completes_state(
    config: job.IndicatorsJobConfig,
) -> None:
    spark = MagicMock()
    source_df = MagicMock()
    source_df.limit.return_value.count.return_value = 10

    summaries = MagicMock()
    sector_perf = MagicMock()
    correlations = MagicMock()
    for frame, count in ((summaries, 10), (sector_perf, 3), (correlations, 5)):
        frame.withColumn.return_value = frame
        frame.count.return_value = count

    with patch.object(job, "read_source", return_value=(source_df, 3)), patch.object(
        job, "compute_daily_summaries", return_value=summaries
    ), patch.object(
        job, "compute_sector_performance", return_value=sector_perf
    ), patch.object(
        job, "compute_correlation_matrix", return_value=correlations
    ), patch.object(
        job, "write_snapshot"
    ) as write, patch.object(
        job, "table_version", return_value=9
    ), patch.object(
        job, "F"
    ):
        counts = job.rebuild_outputs(spark, config)

    assert counts == {
        "daily_summaries_rows": 10,
        "sector_performance_rows": 3,
        "correlations_rows": 5,
    }
    assert [call.args[2] for call in write.call_args_list] == [
        "state",
        "daily_summaries",
        "sector_performance",
        "correlations",
        "state",
    ]
    states = [call.args[0][0][0] for call in spark.createDataFrame.call_args_list]
    assert states == ["processing", "completed"]


# ---------------------------------------------------------------------------
# run / main
# ---------------------------------------------------------------------------
def test_run_always_rebuilds(config: job.IndicatorsJobConfig) -> None:
    spark = MagicMock()
    with patch.object(job, "validate_locations") as validate, patch.object(
        job, "rebuild_outputs", return_value={"daily_summaries_rows": 3}
    ) as rebuild:
        result = job.run(spark, config)
    validate.assert_called_once_with(spark, config)
    rebuild.assert_called_once_with(spark, config)
    assert result == {"daily_summaries_rows": 3}
    spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")


def test_entrypoint_passes_explicit_job_parameters() -> None:
    with patch(
        "sys.argv",
        [
            "indicators",
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
    deployed = bundle["resources"]["jobs"]["landed_indicators"]
    assert not {"schedule", "trigger", "continuous"}.intersection(deployed)
    assert deployed["max_concurrent_runs"] == 1
    assert deployed["queue"]["enabled"] is False
    task = deployed["tasks"][0]
    assert task["max_retries"] == 0
    assert task["python_wheel_task"]["entry_point"] == "landed_indicators"
    assert "job_clusters" not in deployed
    assert "run_as" not in bundle
    package = tomllib.loads((root / "databricks/pyproject.toml").read_text())
    assert (
        package["project"]["scripts"]["landed_indicators"]
        == "src.batch.databricks_indicators:main"
    )
