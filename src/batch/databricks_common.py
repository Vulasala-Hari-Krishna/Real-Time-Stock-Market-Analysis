"""Shared constants for Databricks job runners (R1/R6 ticks/fundamentals/
historical/indicators).

Auto Optimize replaces the legacy delta_maintenance.py (OPTIMIZE + VACUUM)
job: rather than a separate scheduled maintenance job, these two Delta
table properties compact small files during/after writes automatically.
VACUUM itself is not replicated here - at this project's data scale, file
bloat is not a practical concern; run it ad-hoc via SQL if it ever becomes
one, per snowflake/README.md-style guidance in databricks/README.md.
"""

DELTA_AUTO_OPTIMIZE_PROPERTIES: dict[str, str] = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}
