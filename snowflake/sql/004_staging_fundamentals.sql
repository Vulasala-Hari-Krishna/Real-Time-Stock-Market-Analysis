-- Isolated staging table for the fundamentals snapshot dataset (R6).
-- Loaded fresh from the exact manifest-listed files on every run (see
-- src/load/snowflake_snapshot.py::copy_into_staging); never read directly by
-- dashboards/marts - only the SERVING table (005) is the published output.
-- Column set mirrors src/common/schemas.py::FundamentalData plus the
-- bronze_version lineage column src/batch/databricks_fundamentals.py's
-- rebuild_outputs appends. Business key is (symbol) only - one row per
-- symbol, the latest known snapshot, not a time series.
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.FUNDAMENTALS_STAGING (
    symbol                 VARCHAR NOT NULL,
    retrieved_at           TIMESTAMP_NTZ NOT NULL,
    market_cap             DOUBLE,
    pe_ratio               DOUBLE,
    forward_pe             DOUBLE,
    dividend_yield         DOUBLE,
    eps                    DOUBLE,
    beta                   DOUBLE,
    fifty_two_week_high    DOUBLE,
    fifty_two_week_low     DOUBLE,
    sector                 VARCHAR,
    industry               VARCHAR,
    bronze_version         NUMBER(38, 0)
);
