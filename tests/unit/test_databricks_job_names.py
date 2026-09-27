"""Unit tests for dags/databricks_job_names.py (R8).

Imported as ``dags.databricks_job_names`` here because pytest resolves it
from the repo root (already on sys.path for every other test module in this
suite); the DAG modules that use it at Airflow runtime instead import it as
a bare sibling module (``from databricks_job_names import job_name``),
because Airflow's DagBag loader puts each DAG file's own containing folder
directly on sys.path. Same file, two different valid import spellings for
two different runtimes.
"""

import pytest

from dags.databricks_job_names import job_name


def test_job_name_uses_the_configured_bundle_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_BUNDLE_TARGET", "dev")
    assert job_name("landed-ticks") == "stock-market-hybrid-dev-landed-ticks"


def test_job_name_reflects_a_non_default_bundle_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_BUNDLE_TARGET", "prod")
    assert job_name("landed-fundamentals") == (
        "stock-market-hybrid-prod-landed-fundamentals"
    )
