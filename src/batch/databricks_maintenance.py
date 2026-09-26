"""Scheduled Delta table maintenance (OPTIMIZE + VACUUM) across every table
this migration owns.

This is the one job in this migration that runs on a real, native Databricks
schedule rather than being manually/externally triggered - a deliberate
exception, not an inconsistency. Every business-logic job here (ticks,
fundamentals, historical, indicators) is left unscheduled, manually
triggered, because it processes/publishes business data and this project's
standing rule is that only Databricks compute - never GitHub Actions -
executes that logic, with *when* it runs left to a human or (eventually)
local Airflow (R8). Maintenance is different: it produces no business data,
has no downstream data-correctness dependency on being triggered by anything
external, and real production Delta pipelines run OPTIMIZE/VACUUM on a
schedule as a matter of course, regardless of data volume - "personal
project scale" is not a reason to skip it if the goal is matching real-world
architecture (this was the user's own explicit correction, 2026-09-26).

Auto Optimize (delta.autoOptimize.optimizeWrite/autoCompact, set on every
write across all four jobs - see databricks_common.py) compacts small files
during writes. It does not replace VACUUM: every `write_snapshot`
(``mode="overwrite"``) leaves the *previous* run's now-superseded Parquet
files on disk, referenced only by old, no-longer-current Delta versions -
real storage cost that accumulates with every run regardless of row count.
VACUUM is what actually deletes those stale files once they age past the
retention window.

Never bypasses Delta's retention safety check (which blocks VACUUM below
168 hours / 7 days unless a session config disables it) - 168 hours is the
floor here too, matching standard safe practice, not a personal-project
shortcut.

The job's own bundle schedule ships PAUSED by default (see
databricks/databricks.yml) - `bundle deploy` never silently starts a live
recurring cron job; unpausing it is a separate, deliberate action.
"""

import argparse
import logging
from typing import Any, Mapping

from pyspark.sql import SparkSession

from src.batch import (
    databricks_fundamentals,
    databricks_historical,
    databricks_indicators,
    databricks_ticks,
)

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_HOURS = 168  # Delta's own safe-by-default floor (7 days).

# Every Delta table this migration owns, gathered from each job's own TABLES
# registry rather than hand-duplicated here - if a job adds a table, this
# job automatically maintains it too. Value key type is `Any`, not `str`:
# each job's own TABLES dict is keyed by its own Literal dataset-name type,
# and dict/Mapping key parameters are invariant, so only Any lines up here
# without a cast.
OWNED_TABLES_BY_JOB: dict[str, Mapping[Any, tuple[str, str]]] = {
    "ticks": databricks_ticks.TABLES,
    "fundamentals": databricks_fundamentals.TABLES,
    "historical": databricks_historical.TABLES,
    "indicators": databricks_indicators.TABLES,
}


def all_owned_tables(catalog: str, schema_prefix: str) -> list[str]:
    """Enumerate every Delta table owned by this migration's Databricks jobs.

    Args:
        catalog: Existing Unity Catalog catalog.
        schema_prefix: Prefix shared by every job's bronze/silver/gold schemas.

    Returns:
        Fully-qualified ``catalog.schema.table`` identifiers, one per owned
        table across every job.
    """
    names: list[str] = []
    for tables in OWNED_TABLES_BY_JOB.values():
        for layer, name in tables.values():
            names.append(f"{catalog}.{schema_prefix}_{layer}.{name}")
    return names


def optimize_and_vacuum(
    spark: SparkSession, table: str, retain_hours: int = DEFAULT_RETENTION_HOURS
) -> dict[str, str]:
    """Run OPTIMIZE then VACUUM on one table; never raises.

    Args:
        spark: Runtime session.
        table: Fully-qualified table identifier.
        retain_hours: VACUUM retention window; never below Delta's own
            168-hour safety floor (enforced by the caller's config, not
            re-checked here since Delta itself also enforces it server-side).

    Returns:
        ``{"table": ..., "status": "ok"}`` or
        ``{"table": ..., "status": "failed", "error": ...}`` - a failure on
        one table never raises, so it never blocks maintaining the rest.
    """
    try:
        logger.info("Running OPTIMIZE on %s", table)
        spark.sql(f"OPTIMIZE {table}")
        logger.info("Running VACUUM on %s (RETAIN %d HOURS)", table, retain_hours)
        spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
        return {"table": table, "status": "ok"}
    except Exception as exc:
        logger.exception("Maintenance failed for %s", table)
        return {"table": table, "status": "failed", "error": str(exc)}


def run_maintenance(
    spark: SparkSession,
    catalog: str,
    schema_prefix: str,
    retain_hours: int = DEFAULT_RETENTION_HOURS,
) -> dict[str, dict[str, str]]:
    """Run OPTIMIZE + VACUUM across every owned table; one failure doesn't
    block the rest.

    Args:
        spark: Runtime session.
        catalog: Existing Unity Catalog catalog.
        schema_prefix: Prefix shared by every job's bronze/silver/gold schemas.
        retain_hours: VACUUM retention window in hours.

    Returns:
        One result dict per table, keyed by its fully-qualified name.

    Raises:
        ValueError: If retain_hours is below Delta's own 168-hour safety
            floor - fails fast with a clear message instead of letting
            Delta itself reject it later with a more cryptic error.
        RuntimeError: If any table's maintenance failed - after attempting
            every table, so a real failure is never silently swallowed, but
            one bad table also never prevents maintaining the healthy ones.
    """
    if retain_hours < DEFAULT_RETENTION_HOURS:
        raise ValueError(
            f"retain_hours={retain_hours} is below Delta's 168-hour (7-day) "
            "safety floor; this is never bypassed"
        )
    results = {
        table: optimize_and_vacuum(spark, table, retain_hours)
        for table in all_owned_tables(catalog, schema_prefix)
    }
    failed = [t for t, r in results.items() if r["status"] == "failed"]
    logger.info(
        "Maintenance complete: %d/%d tables ok",
        len(results) - len(failed),
        len(results),
    )
    if failed:
        raise RuntimeError(f"Maintenance failed for {len(failed)} table(s): {failed}")
    return results


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument(
        "--retain-hours",
        type=int,
        default=DEFAULT_RETENTION_HOURS,
        help=(
            "VACUUM retention window in hours. Never set below Delta's own "
            "168-hour (7-day) safety floor - this is not bypassed."
        ),
    )
    return parser.parse_args(argv)


def main() -> None:
    """Run only when explicitly invoked by a Databricks wheel task/schedule."""
    args = _parse_args()
    # force=True: Databricks Runtime configures the root logger before this
    # code ever runs, and basicConfig() silently no-ops if the root logger
    # already has handlers (found live 2026-09-26 in databricks_fundamentals.py).
    logging.basicConfig(level=logging.INFO, force=True)
    run_maintenance(
        SparkSession.builder.getOrCreate(),
        args.catalog,
        args.schema_prefix,
        retain_hours=args.retain_hours,
    )


if __name__ == "__main__":
    main()
