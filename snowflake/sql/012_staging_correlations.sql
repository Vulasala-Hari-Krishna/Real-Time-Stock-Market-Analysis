-- Isolated staging table for the correlations snapshot dataset (R6).
-- Business key is (symbol_a, symbol_b, date), matching the legacy
-- product's documented MERGE key (see README.md#merge-keys).
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.CORRELATIONS_STAGING (
    date            DATE NOT NULL,
    symbol_a        VARCHAR NOT NULL,
    symbol_b        VARCHAR NOT NULL,
    correlation     DOUBLE,
    source_version  NUMBER(38, 0)
);
