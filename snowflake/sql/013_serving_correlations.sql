-- Published, queryable output of the correlations snapshot loader (R6).
-- Replaced transactionally (DELETE + INSERT, never TRUNCATE/CREATE OR
-- REPLACE inside it) so a failed load leaves the previous snapshot intact.
-- Business key is (symbol_a, symbol_b, date).
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.CORRELATIONS (
    date            DATE NOT NULL,
    symbol_a        VARCHAR NOT NULL,
    symbol_b        VARCHAR NOT NULL,
    correlation     DOUBLE,
    source_version  NUMBER(38, 0),
    loaded_batch_id VARCHAR NOT NULL,
    loaded_at       TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT correlations_pk PRIMARY KEY (symbol_a, symbol_b, date)
);
