-- Isolated staging table for the sector_performance snapshot dataset (R6).
-- Business key is (sector, date), matching the legacy product's documented
-- MERGE key (see README.md#merge-keys).
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.SECTOR_PERFORMANCE_STAGING (
    sector           VARCHAR NOT NULL,
    date             DATE NOT NULL,
    avg_return_pct   DOUBLE,
    top_performer    VARCHAR,
    bottom_performer VARCHAR,
    source_version   NUMBER(38, 0)
);
