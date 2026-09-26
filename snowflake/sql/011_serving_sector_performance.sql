-- Published, queryable output of the sector_performance snapshot loader
-- (R6). Replaced transactionally (DELETE + INSERT, never TRUNCATE/CREATE OR
-- REPLACE inside it) so a failed load leaves the previous snapshot intact.
-- Business key is (sector, date).
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.SECTOR_PERFORMANCE (
    sector           VARCHAR NOT NULL,
    date             DATE NOT NULL,
    avg_return_pct   DOUBLE,
    top_performer    VARCHAR,
    bottom_performer VARCHAR,
    source_version   NUMBER(38, 0),
    loaded_batch_id  VARCHAR NOT NULL,
    loaded_at        TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT sector_performance_pk PRIMARY KEY (sector, date)
);
