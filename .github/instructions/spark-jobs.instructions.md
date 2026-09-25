---
applyTo: "src/consumers/**/*.py,src/batch/**/*.py,databricks/**/*.py"
description: "Use when changing local Spark consumers, reusable batch transformations, Delta writes, or Databricks Spark job code."
---

# Spark Job Instructions

- Follow [the hybrid architecture](../copilot-instructions.md). Preserve Spark 3.5-compatible local behavior; select and verify a compatible Databricks runtime instead of globally upgrading Spark during migration.
- Separate reusable DataFrame transformations from session setup, I/O, and CLI/job entry points. Reuse existing indicator/rollup logic and tests.
- The local consumer remains the Kafka bridge. Its target S3 output is immutable raw messages plus topic/partition/offset and timestamps; canonical validation/quarantine/deduplication belongs in Databricks. Keep the old silver Parquet path working until an explicit migration replaces it.
- Batch small uploads by time/size rather than emitting an object per quote. Retain enough source identity for replay detection and advance processing progress only after durable persistence.
- Use `readStream`/`writeStream` for streaming and durable, unique checkpoints per query. Preserve state across restarts and keep checkpoints outside expiring prefixes.
- `foreachBatch` alone does not guarantee exactly-once delivery. Make its sink idempotent using stable batch/source identities or supported Delta transaction semantics; test failures between write completion and checkpoint advancement.
- Define explicit schemas for canonical data. Auto Loader may use controlled inference/evolution at raw ingestion, with persisted schema state and rescued/quarantined records; do not silently evolve serving contracts.
- Target bronze/silver/gold writes use Delta and supported Delta APIs. Existing silver Parquet, raw landing formats, and explicit Parquet exports are distinct contracts; never read Delta directories as ordinary Parquet after a Delta-read error.
- Use native Delta streaming sinks when appropriate; use `foreachBatch` only when its custom logic is needed. Databricks ingestion defaults to `AvailableNow`, not an always-running processing-time trigger.
- Upsert on explicit business keys and deduplicate the merge source. Preserve indicator warm-up windows; historical corrections require recomputing affected dates and downstream windows, not only merging the latest date.
- Define event-time lateness and watermark behavior where stateful streaming requires it. Watermark deduplication is bounded and does not replace durable replay protection.
- Choose partitioning/layout based on dataset size and access patterns, not mandatory per-symbol/day partitions for every small table. Preserve useful pruning in existing local Parquet jobs; avoid excessive small files.
- Avoid unbounded `collect()`/`toPandas()` and unnecessary full scans. Log progress and bounded validation metrics without collecting complete datasets.
- Use Delta-aware maintenance and retention compatible with replay/time-travel requirements. Do not schedule compaction merely to exercise a feature, or vacuum files needed by downstream recovery.
- Databricks supplies Spark/Delta runtime dependencies and session configuration. Do not inject the standalone JAR/catalog configuration or static S3 keys into remote jobs; use Unity Catalog-supported access.