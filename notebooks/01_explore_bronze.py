# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Explore Bronze Layer
# MAGIC
# MAGIC This notebook reads the raw JSON data from the **bronze** layer in S3 and
# MAGIC performs initial exploration: schema inspection, sample records, and basic
# MAGIC descriptive statistics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BUCKET = spark.conf.get("spark.stock.s3_bucket", "stock-market-datalake")
BRONZE_PATH = f"s3a://{BUCKET}/bronze/historical"

WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze JSON

# COMMAND ----------

bronze_df = spark.read.json(BRONZE_PATH)
print(f"Total rows: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Inspection

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Records

# COMMAND ----------

display(bronze_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Counts per Symbol

# COMMAND ----------

from pyspark.sql import functions as F

symbol_counts = (
    bronze_df.groupBy("symbol")
    .agg(
        F.count("*").alias("row_count"),
        F.min("date").alias("earliest_date"),
        F.max("date").alias("latest_date"),
    )
    .orderBy("symbol")
)
display(symbol_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Statistics

# COMMAND ----------

numeric_cols = ["open", "high", "low", "close", "volume"]
available_cols = [c for c in numeric_cols if c in bronze_df.columns]

display(bronze_df.select(available_cols).describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null / Missing Value Check

# COMMAND ----------

null_counts = bronze_df.select(
    [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in bronze_df.columns]
)
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition Listing
# MAGIC
# MAGIC Check how the data is partitioned on disk.

# COMMAND ----------

partitions = (
    bronze_df.select("symbol")
    .distinct()
    .orderBy("symbol")
)
print(f"Distinct symbols: {partitions.count()}")
display(partitions)
