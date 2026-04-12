"""Unit tests for the Delta Lake maintenance batch job.

All external I/O (Spark, Delta, S3) is mocked.  Tests cover table
detection, OPTIMIZE, VACUUM, history logging, and the full pipeline.
"""

from unittest.mock import MagicMock, call, patch

from src.batch.delta_maintenance import (
    DEFAULT_RETENTION_HOURS,
    GOLD_TABLES,
    _is_delta_table,
    log_table_history,
    optimize_table,
    run_maintenance,
    vacuum_table,
)


# ---------------------------------------------------------------------------
# Constants tests
# ---------------------------------------------------------------------------


class TestConstants:
    """Tests for module-level constants."""

    def test_gold_tables_count(self) -> None:
        """All five gold tables are listed."""
        assert len(GOLD_TABLES) == 5

    def test_gold_tables_contents(self) -> None:
        """Expected table names are present."""
        expected = {
            "daily_summaries",
            "sector_performance",
            "correlations",
            "fundamentals",
            "enriched_prices",
        }
        assert set(GOLD_TABLES) == expected

    def test_default_retention_hours(self) -> None:
        """Default retention is 7 days (168 hours)."""
        assert DEFAULT_RETENTION_HOURS == 168


# ---------------------------------------------------------------------------
# _is_delta_table tests
# ---------------------------------------------------------------------------


class TestIsDeltaTable:
    """Tests for Delta table detection."""

    def test_returns_true_when_delta(self) -> None:
        """Returns True when DeltaTable.isDeltaTable returns True."""
        mock_spark = MagicMock()
        mock_delta = MagicMock()
        mock_delta.isDeltaTable.return_value = True

        with patch.dict(
            "sys.modules",
            {"delta": MagicMock(), "delta.tables": mock_delta},
        ):
            mock_delta.DeltaTable = mock_delta
            result = _is_delta_table(mock_spark, "s3a://bucket/gold/test")

        assert result is True

    def test_returns_false_on_exception(self) -> None:
        """Returns False when the check raises an exception."""
        mock_spark = MagicMock()
        mock_delta = MagicMock()
        mock_delta.DeltaTable.isDeltaTable.side_effect = Exception("no table")

        with patch.dict(
            "sys.modules",
            {"delta": MagicMock(), "delta.tables": mock_delta},
        ):
            result = _is_delta_table(mock_spark, "s3a://bucket/gold/missing")

        assert result is False


# ---------------------------------------------------------------------------
# optimize_table tests
# ---------------------------------------------------------------------------


class TestOptimizeTable:
    """Tests for the OPTIMIZE operation."""

    def test_runs_optimize_sql(self) -> None:
        """Executes OPTIMIZE SQL against the table path."""
        mock_spark = MagicMock()
        path = "s3a://bucket/gold/daily_summaries"

        optimize_table(mock_spark, path)

        mock_spark.sql.assert_called_once_with(f"OPTIMIZE delta.`{path}`")


# ---------------------------------------------------------------------------
# vacuum_table tests
# ---------------------------------------------------------------------------


class TestVacuumTable:
    """Tests for the VACUUM operation."""

    def test_runs_vacuum_sql_default(self) -> None:
        """Executes VACUUM SQL with default retention."""
        mock_spark = MagicMock()
        path = "s3a://bucket/gold/fundamentals"

        vacuum_table(mock_spark, path)

        mock_spark.sql.assert_called_once_with(
            f"VACUUM delta.`{path}` RETAIN {DEFAULT_RETENTION_HOURS} HOURS"
        )

    def test_runs_vacuum_sql_custom_retention(self) -> None:
        """Executes VACUUM SQL with custom retention hours."""
        mock_spark = MagicMock()
        path = "s3a://bucket/gold/correlations"

        vacuum_table(mock_spark, path, retention_hours=336)

        mock_spark.sql.assert_called_once_with(
            f"VACUUM delta.`{path}` RETAIN 336 HOURS"
        )


# ---------------------------------------------------------------------------
# log_table_history tests
# ---------------------------------------------------------------------------


class TestLogTableHistory:
    """Tests for Delta history logging."""

    def test_returns_version_and_count(self) -> None:
        """Returns current version and history entry count."""
        mock_spark = MagicMock()
        mock_dt = MagicMock()
        mock_history = MagicMock()
        mock_dt.history.return_value = mock_history
        mock_history.count.return_value = 5
        mock_history.selectExpr.return_value.collect.return_value = [MagicMock()]
        mock_history.selectExpr.return_value.collect.return_value[0].__getitem__ = (
            lambda self, idx: 4
        )

        mock_delta_mod = MagicMock()
        mock_delta_mod.DeltaTable.forPath.return_value = mock_dt

        with patch.dict(
            "sys.modules",
            {"delta": MagicMock(), "delta.tables": mock_delta_mod},
        ):
            result = log_table_history(mock_spark, "s3a://bucket/gold/t")

        assert result["current_version"] == 4
        assert result["history_entries"] == 5


# ---------------------------------------------------------------------------
# run_maintenance tests
# ---------------------------------------------------------------------------


class TestRunMaintenance:
    """Tests for the full maintenance pipeline."""

    @patch("src.batch.delta_maintenance.log_table_history")
    @patch("src.batch.delta_maintenance.vacuum_table")
    @patch("src.batch.delta_maintenance.optimize_table")
    @patch("src.batch.delta_maintenance._is_delta_table")
    @patch("src.batch.delta_maintenance.get_settings")
    def test_processes_all_existing_tables(
        self,
        mock_settings: MagicMock,
        mock_is_delta: MagicMock,
        mock_optimize: MagicMock,
        mock_vacuum: MagicMock,
        mock_history: MagicMock,
    ) -> None:
        """All tables are processed when they exist."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_is_delta.return_value = True
        mock_history.return_value = {"current_version": 3, "history_entries": 4}
        mock_spark = MagicMock()

        results = run_maintenance(spark=mock_spark)

        assert len(results) == 5
        assert mock_optimize.call_count == 5
        assert mock_vacuum.call_count == 5
        assert mock_history.call_count == 5

    @patch("src.batch.delta_maintenance.log_table_history")
    @patch("src.batch.delta_maintenance.vacuum_table")
    @patch("src.batch.delta_maintenance.optimize_table")
    @patch("src.batch.delta_maintenance._is_delta_table")
    @patch("src.batch.delta_maintenance.get_settings")
    def test_skips_non_delta_tables(
        self,
        mock_settings: MagicMock,
        mock_is_delta: MagicMock,
        mock_optimize: MagicMock,
        mock_vacuum: MagicMock,
        mock_history: MagicMock,
    ) -> None:
        """Tables that aren't Delta are skipped."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_is_delta.return_value = False
        mock_spark = MagicMock()

        results = run_maintenance(spark=mock_spark)

        assert len(results) == 0
        mock_optimize.assert_not_called()
        mock_vacuum.assert_not_called()

    @patch("src.batch.delta_maintenance.log_table_history")
    @patch("src.batch.delta_maintenance.vacuum_table")
    @patch("src.batch.delta_maintenance.optimize_table")
    @patch("src.batch.delta_maintenance._is_delta_table")
    @patch("src.batch.delta_maintenance.get_settings")
    def test_partial_delta_tables(
        self,
        mock_settings: MagicMock,
        mock_is_delta: MagicMock,
        mock_optimize: MagicMock,
        mock_vacuum: MagicMock,
        mock_history: MagicMock,
    ) -> None:
        """Only Delta tables are maintained when some don't exist."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        # Only first 2 tables are Delta
        mock_is_delta.side_effect = [True, True, False, False, False]
        mock_history.return_value = {"current_version": 1, "history_entries": 2}
        mock_spark = MagicMock()

        results = run_maintenance(spark=mock_spark)

        assert len(results) == 2
        assert mock_optimize.call_count == 2
        assert mock_vacuum.call_count == 2

    @patch("src.batch.delta_maintenance.log_table_history")
    @patch("src.batch.delta_maintenance.vacuum_table")
    @patch("src.batch.delta_maintenance.optimize_table")
    @patch("src.batch.delta_maintenance._is_delta_table")
    @patch("src.batch.delta_maintenance.get_settings")
    def test_custom_retention_hours(
        self,
        mock_settings: MagicMock,
        mock_is_delta: MagicMock,
        mock_optimize: MagicMock,
        mock_vacuum: MagicMock,
        mock_history: MagicMock,
    ) -> None:
        """Custom retention hours are passed to VACUUM."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_is_delta.return_value = True
        mock_history.return_value = {"current_version": 1, "history_entries": 1}
        mock_spark = MagicMock()

        run_maintenance(spark=mock_spark, retention_hours=336)

        for c in mock_vacuum.call_args_list:
            assert c == call(mock_spark, c[0][1], 336)

    @patch("src.batch.delta_maintenance.create_spark_session")
    @patch("src.batch.delta_maintenance.log_table_history")
    @patch("src.batch.delta_maintenance.vacuum_table")
    @patch("src.batch.delta_maintenance.optimize_table")
    @patch("src.batch.delta_maintenance._is_delta_table")
    @patch("src.batch.delta_maintenance.get_settings")
    def test_creates_spark_session_when_none(
        self,
        mock_settings: MagicMock,
        mock_is_delta: MagicMock,
        mock_optimize: MagicMock,
        mock_vacuum: MagicMock,
        mock_history: MagicMock,
        mock_create_spark: MagicMock,
    ) -> None:
        """SparkSession is created when not provided."""
        mock_settings.return_value.s3_bucket_name = "test-bucket"
        mock_is_delta.return_value = False

        run_maintenance(spark=None)

        mock_create_spark.assert_called_once()
