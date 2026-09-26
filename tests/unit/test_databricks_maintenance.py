"""Unit tests for src/batch/databricks_maintenance.py (scheduled OPTIMIZE
+ VACUUM across every owned Delta table)."""

from unittest.mock import MagicMock, patch

import pytest

from src.batch import databricks_maintenance as job


# ---------------------------------------------------------------------------
# all_owned_tables
# ---------------------------------------------------------------------------
def test_all_owned_tables_covers_every_job() -> None:
    tables = job.all_owned_tables("portfolio", "stocks")

    assert "portfolio.stocks_bronze.ticks_raw" in tables
    assert "portfolio.stocks_gold.daily_quote_summary" in tables
    assert "portfolio.stocks_bronze.fundamentals_raw" in tables
    assert "portfolio.stocks_gold.fundamentals" in tables
    assert "portfolio.stocks_bronze.historical_ohlcv_raw" in tables
    assert "portfolio.stocks_gold.historical_ohlcv" in tables
    assert "portfolio.stocks_gold.daily_summaries" in tables
    assert "portfolio.stocks_gold.sector_performance" in tables
    assert "portfolio.stocks_gold.correlations" in tables
    # No duplicates - every owned table appears exactly once.
    assert len(tables) == len(set(tables))


# ---------------------------------------------------------------------------
# optimize_and_vacuum
# ---------------------------------------------------------------------------
def test_optimize_and_vacuum_runs_optimize_then_vacuum() -> None:
    spark = MagicMock()
    result = job.optimize_and_vacuum(spark, "cat.schema.table", retain_hours=168)

    assert result == {"table": "cat.schema.table", "status": "ok"}
    calls = [call.args[0] for call in spark.sql.call_args_list]
    assert calls == [
        "OPTIMIZE cat.schema.table",
        "VACUUM cat.schema.table RETAIN 168 HOURS",
    ]


def test_optimize_and_vacuum_never_raises_on_failure() -> None:
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("boom")

    result = job.optimize_and_vacuum(spark, "cat.schema.table")

    assert result == {"table": "cat.schema.table", "status": "failed", "error": "boom"}


# ---------------------------------------------------------------------------
# run_maintenance
# ---------------------------------------------------------------------------
def test_run_maintenance_rejects_retention_below_the_safety_floor() -> None:
    spark = MagicMock()
    with pytest.raises(ValueError, match="168-hour"):
        job.run_maintenance(spark, "portfolio", "stocks", retain_hours=24)


def test_run_maintenance_succeeds_when_every_table_is_ok() -> None:
    spark = MagicMock()
    with patch.object(
        job, "all_owned_tables", return_value=["cat.a.t1", "cat.a.t2"]
    ), patch.object(
        job,
        "optimize_and_vacuum",
        side_effect=lambda s, t, retain_hours=168: {"table": t, "status": "ok"},
    ):
        results = job.run_maintenance(spark, "portfolio", "stocks")

    assert results == {
        "cat.a.t1": {"table": "cat.a.t1", "status": "ok"},
        "cat.a.t2": {"table": "cat.a.t2", "status": "ok"},
    }


def test_run_maintenance_attempts_every_table_then_raises_if_any_failed() -> None:
    spark = MagicMock()

    def fake_optimize(s, t, retain_hours=168):
        if t == "cat.a.bad":
            return {"table": t, "status": "failed", "error": "boom"}
        return {"table": t, "status": "ok"}

    with patch.object(
        job, "all_owned_tables", return_value=["cat.a.good", "cat.a.bad"]
    ), patch.object(job, "optimize_and_vacuum", side_effect=fake_optimize) as mock_ov:
        with pytest.raises(RuntimeError, match="cat.a.bad"):
            job.run_maintenance(spark, "portfolio", "stocks")

    # Both tables were attempted despite the failure - one bad table never
    # blocks maintaining the rest.
    assert mock_ov.call_count == 2


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def test_entrypoint_passes_explicit_job_parameters() -> None:
    with patch(
        "sys.argv",
        ["maintenance", "--catalog", "portfolio", "--schema-prefix", "stocks"],
    ), patch.object(job, "SparkSession"), patch.object(job, "run_maintenance") as run:
        job.main()
    assert run.call_args.args[1] == "portfolio"
    assert run.call_args.args[2] == "stocks"
    assert run.call_args.kwargs["retain_hours"] == job.DEFAULT_RETENTION_HOURS


def test_bundle_schedule_ships_paused_by_default() -> None:
    from pathlib import Path

    import tomllib
    import yaml

    root = Path(__file__).resolve().parents[2]
    bundle = yaml.safe_load((root / "databricks/databricks.yml").read_text())
    deployed = bundle["resources"]["jobs"]["maintain_delta_tables"]
    # This is the one job in the bundle that IS scheduled - deliberately,
    # not an oversight - but must never auto-activate on a plain deploy.
    assert deployed["schedule"]["pause_status"] == "${var.maintenance_schedule_status}"
    assert bundle["variables"]["maintenance_schedule_status"]["default"] == "PAUSED"
    task = deployed["tasks"][0]
    assert task["python_wheel_task"]["entry_point"] == "maintain_delta_tables"
    assert "run_as" not in bundle
    package = tomllib.loads((root / "databricks/pyproject.toml").read_text())
    assert (
        package["project"]["scripts"]["maintain_delta_tables"]
        == "src.batch.databricks_maintenance:main"
    )
