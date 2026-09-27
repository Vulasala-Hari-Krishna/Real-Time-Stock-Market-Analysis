"""Unit tests for dashboards/local_cache.py (R7 durable local dashboard cache)."""

from pathlib import Path

import pandas as pd
import pytest

from dashboards import local_cache


@pytest.fixture(autouse=True)
def _isolate_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Never touch the real .state/dashboard-cache directory in tests."""
    monkeypatch.setattr(local_cache, "CACHE_DIR", tmp_path / "dashboard-cache")


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({"symbol": ["AAPL", "MSFT"], "price": [150.0, 420.0]})


def test_load_snapshot_returns_none_when_nothing_was_ever_saved() -> None:
    assert local_cache.load_snapshot("daily_quote_summary") is None


def test_save_then_load_round_trips_data_and_metadata() -> None:
    df = _sample_df()
    metadata = {"batch_id": "batch-1", "as_of": "2026-01-01T00:00:00+00:00"}

    local_cache.save_snapshot("daily_quote_summary", df, metadata)
    result = local_cache.load_snapshot("daily_quote_summary")

    assert result is not None
    loaded_df, loaded_metadata = result
    pd.testing.assert_frame_equal(loaded_df, df)
    assert loaded_metadata == metadata


def test_save_overwrites_the_previous_snapshot_not_accumulates() -> None:
    local_cache.save_snapshot("daily_quote_summary", _sample_df(), {"batch_id": "b1"})
    second_df = pd.DataFrame({"symbol": ["GOOGL"], "price": [155.0]})
    local_cache.save_snapshot("daily_quote_summary", second_df, {"batch_id": "b2"})

    result = local_cache.load_snapshot("daily_quote_summary")

    assert result is not None
    loaded_df, loaded_metadata = result
    pd.testing.assert_frame_equal(loaded_df, second_df)
    assert loaded_metadata == {"batch_id": "b2"}
    # Bounded: exactly one data file and one metadata file for this dataset.
    cache_files = list(local_cache.CACHE_DIR.glob("daily_quote_summary.*"))
    assert len(cache_files) == 2


def test_load_snapshot_returns_none_for_a_corrupt_metadata_file() -> None:
    local_cache.save_snapshot("daily_quote_summary", _sample_df(), {"batch_id": "b1"})
    _, meta_path = local_cache._paths("daily_quote_summary")
    meta_path.write_text("{not valid json")

    assert local_cache.load_snapshot("daily_quote_summary") is None


def test_save_snapshot_never_raises_on_an_unwritable_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(local_cache.Path, "mkdir", _boom)

    local_cache.save_snapshot("daily_quote_summary", _sample_df(), {"batch_id": "b1"})

    assert local_cache.load_snapshot("daily_quote_summary") is None
