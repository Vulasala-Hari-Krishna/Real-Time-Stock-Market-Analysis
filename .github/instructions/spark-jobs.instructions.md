---
applyTo: "src/consumers/**/*.py,src/batch/**/*.py"
---

# Spark Job Instructions

- Use PySpark with Spark 3.5 APIs
- For streaming: use Spark Structured Streaming with `readStream` / `writeStream`
- For batch: use `spark.read` / `df.write`
- Always define explicit schemas (`StructType`) — never rely on schema inference in production
- Use `.option("checkpointLocation", ...)` for all streaming writes
- Write output as Parquet format with snappy compression
- Partition output by date columns (year, month, or day) for efficient Athena queries
- Use `foreachBatch` for streaming writes to S3 (micro-batch to Parquet)
- Avoid `collect()` and `toPandas()` on large DataFrames — process distributed
- Log Spark job progress using Python `logging`, not print
- Handle late data with watermarking in streaming jobs