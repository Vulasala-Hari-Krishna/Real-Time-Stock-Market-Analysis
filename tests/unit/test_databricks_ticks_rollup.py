"""Cloud-free runner boundary tests; Spark semantics are checked separately."""

from unittest.mock import MagicMock, patch

import pytest

from src.batch import databricks_historical
from src.batch import databricks_ticks_rollup as job


@pytest.fixture()
def config() -> databricks_historical.HistoricalJobConfig:
    return databricks_historical.HistoricalJobConfig(
        catalog="portfolio", schema_prefix="stocks", bucket="test-bucket"
    )


def test_source_table_points_at_ticks_gold_summary(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    assert job.source_table(config) == "portfolio.stocks_gold.daily_quote_summary"


def test_read_source_pins_version_and_selects_quote_summary_columns(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    reader = spark.read.option.return_value
    selected = reader.table.return_value.select.return_value
    with patch.object(job, "table_version", return_value=6):
        df, version = job.read_source(spark, config)

    assert version == 6
    spark.read.option.assert_called_once_with("versionAsOf", 6)
    reader.table.assert_called_once_with(job.source_table(config))
    reader.table.return_value.select.assert_called_once_with(
        "symbol",
        "capture_date_utc",
        "first_observed_price",
        "highest_observed_price",
        "lowest_observed_price",
        "last_observed_price",
        "last_reported_volume",
    )
    assert df is selected


def test_append_bronze_from_frame_appends_and_returns_new_version(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    frame = MagicMock()
    frame.withColumn.return_value = frame
    writer = frame.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.options.return_value = writer
    spark = MagicMock()
    with patch.object(job, "F"), patch.object(job, "table_version", return_value=7):
        version = job.append_bronze_from_frame(spark, config, frame)

    assert version == 7
    writer.mode.assert_called_once_with("append")
    writer.saveAsTable.assert_called_once_with(config.table("raw"))


# ---------------------------------------------------------------------------
# rebuild_outputs
# ---------------------------------------------------------------------------
def test_rebuild_outputs_refuses_an_empty_source(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    source = MagicMock()
    rolled_up = MagicMock()
    rolled_up.count.return_value = 0
    with patch.object(job, "read_source", return_value=(source, 3)), patch.object(
        job, "project_quote_summary_as_ohlcv", return_value=rolled_up
    ), patch.object(job, "append_bronze_from_frame") as append:
        with pytest.raises(RuntimeError, match="refusing to append an empty"):
            job.rebuild_outputs(spark, config)
    append.assert_not_called()
    first_state = spark.createDataFrame.call_args_list[0].args[0][0]
    assert first_state[0] == "processing"


def test_rebuild_outputs_enforces_the_full_rebuild_size_guard(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    source = MagicMock()
    rolled_up = MagicMock()
    rolled_up.count.return_value = 5
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = config.max_input_rows + 1
    with patch.object(job, "read_source", return_value=(source, 3)), patch.object(
        job, "project_quote_summary_as_ohlcv", return_value=rolled_up
    ), patch.object(job, "append_bronze_from_frame", return_value=9):
        with pytest.raises(ValueError, match="full-rebuild row limit"):
            job.rebuild_outputs(spark, config)
    spark.read.option.assert_called_with("versionAsOf", 9)


def test_rebuild_outputs_publishes_gold_and_completes_state(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    source = MagicMock()
    rolled_up = MagicMock()
    rolled_up.count.return_value = 5
    bronze = spark.read.option.return_value.table.return_value
    bronze.limit.return_value.count.return_value = 5
    latest = MagicMock()
    gold = MagicMock()
    gold.withColumn.return_value = gold
    gold.count.return_value = 5
    with patch.object(job, "read_source", return_value=(source, 3)), patch.object(
        job, "project_quote_summary_as_ohlcv", return_value=rolled_up
    ), patch.object(job, "append_bronze_from_frame", return_value=9), patch.object(
        job, "rank_latest_per_symbol_date", return_value=latest
    ), patch.object(
        job, "project_historical", return_value=gold
    ), patch.object(
        job, "write_snapshot"
    ) as write, patch.object(
        job, "table_version", return_value=12
    ), patch.object(
        job, "F"
    ):
        counts = job.rebuild_outputs(spark, config)

    assert counts == {"rolled_up": 5, "gold_rows": 5}
    assert [call.args[2] for call in write.call_args_list] == [
        "state",
        "summary",
        "state",
    ]
    states = [call.args[0][0][0] for call in spark.createDataFrame.call_args_list]
    assert states == ["processing", "completed"]


# ---------------------------------------------------------------------------
# run / main
# ---------------------------------------------------------------------------
def test_run_validates_locations_then_rebuilds(
    config: databricks_historical.HistoricalJobConfig,
) -> None:
    spark = MagicMock()
    with patch.object(job, "validate_locations") as validate, patch.object(
        job, "rebuild_outputs", return_value={"rolled_up": 3}
    ) as rebuild:
        result = job.run(spark, config)
    validate.assert_called_once_with(spark, config)
    rebuild.assert_called_once_with(spark, config)
    assert result == {"rolled_up": 3}
    spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")


def test_entrypoint_passes_explicit_job_parameters() -> None:
    with patch(
        "sys.argv",
        [
            "ticks_rollup",
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


def test_bundle_matches_legacy_tick_rollup_schedule_and_ships_unpaused() -> None:
    from pathlib import Path

    import tomllib
    import yaml

    root = Path(__file__).resolve().parents[2]
    bundle = yaml.safe_load((root / "databricks/databricks.yml").read_text())
    deployed = bundle["resources"]["jobs"]["landed_ticks_rollup"]
    # Quartz day-of-week uses a 1=SUN convention, so legacy's standard-cron
    # weekdays ("* 1-5", 1=Mon) become "2-6" (2=Mon..6=Fri) here.
    assert deployed["schedule"]["quartz_cron_expression"] == "0 0 6 ? * 2-6"
    # Unlike maintain_delta_tables, this job never calls an external API and
    # carries no yfinance rate-limit/cost risk, so it ships unpaused -
    # matching legacy tick_rollup.py's own always-on production schedule.
    assert deployed["schedule"]["pause_status"] == "UNPAUSED"
    task = deployed["tasks"][0]
    assert task["max_retries"] == 0
    assert task["python_wheel_task"]["entry_point"] == "landed_ticks_rollup"
    environment_key = task["environment_key"]
    environments = {env["environment_key"]: env for env in deployed["environments"]}
    dependencies = environments[environment_key]["spec"]["dependencies"]
    assert any("yfinance" in dep for dep in dependencies)
    assert any("pydantic-settings" in dep for dep in dependencies)
    package = tomllib.loads((root / "databricks/pyproject.toml").read_text())
    assert (
        package["project"]["scripts"]["landed_ticks_rollup"]
        == "src.batch.databricks_ticks_rollup:main"
    )
