"""Unit tests for src/batch/landed_ticks_rollup.py."""

from unittest.mock import MagicMock, patch

from src.batch import landed_ticks_rollup as rollup


def test_project_quote_summary_as_ohlcv_maps_every_source_column() -> None:
    frame = MagicMock()
    with patch.object(rollup, "F") as functions:
        rollup.project_quote_summary_as_ohlcv(frame)

    for column in [
        "symbol",
        "capture_date_utc",
        "first_observed_price",
        "highest_observed_price",
        "lowest_observed_price",
        "last_observed_price",
        "last_reported_volume",
    ]:
        functions.col.assert_any_call(column)
    functions.lit.assert_any_call(rollup.SOURCE_LABEL)
    frame.select.assert_called_once()
