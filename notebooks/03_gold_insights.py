# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer Insights
# MAGIC
# MAGIC Read from the **gold** layer to generate screening reports, sector
# MAGIC analysis, and actionable investment insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BUCKET = spark.conf.get("spark.stock.s3_bucket", "stock-market-datalake")
GOLD_ROOT = f"s3a://{BUCKET}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Tables

# COMMAND ----------

daily_df = spark.read.parquet(f"{GOLD_ROOT}/daily_summaries")
fundamentals_df = spark.read.parquet(f"{GOLD_ROOT}/fundamentals")
sector_df = spark.read.parquet(f"{GOLD_ROOT}/sector_performance")
corr_df = spark.read.parquet(f"{GOLD_ROOT}/correlations")

print(f"Daily summaries : {daily_df.count()} rows")
print(f"Fundamentals    : {fundamentals_df.count()} rows")
print(f"Sector perf     : {sector_df.count()} rows")
print(f"Correlations    : {corr_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest Snapshot per Stock

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

latest_w = Window.partitionBy("symbol").orderBy(F.desc("date"))
latest = (
    daily_df.withColumn("rn", F.row_number().over(latest_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
display(
    latest.select(
        "symbol", "date", "close", "daily_return_pct",
        "rsi_14", "sma_20", "sma_50", "signals",
    ).orderBy("symbol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stock Screener — Oversold + High Volume

# COMMAND ----------

screened = latest.filter(
    (F.col("rsi_14") < 35) | (F.col("volume_vs_avg") > 2.0)
).select(
    "symbol", "close", "rsi_14", "volume_vs_avg", "daily_return_pct", "signals",
)
print(f"Screened stocks: {screened.count()}")
display(screened)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fundamental Valuation Screen
# MAGIC
# MAGIC Find stocks trading below their forward P/E average.

# COMMAND ----------

avg_fwd_pe = fundamentals_df.agg(F.avg("forward_pe")).collect()[0][0]
print(f"Average forward P/E: {avg_fwd_pe:.2f}")

undervalued = fundamentals_df.filter(
    (F.col("forward_pe") < avg_fwd_pe) & (F.col("forward_pe") > 0)
).select(
    "symbol", "market_cap", "pe_ratio", "forward_pe",
    "dividend_yield", "eps", "beta",
).orderBy("forward_pe")
display(undervalued)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sector Return Comparison

# COMMAND ----------

import plotly.express as px

sector_pdf = (
    sector_df.groupBy("sector")
    .agg(F.avg("avg_return_pct").alias("avg_return_pct"))
    .orderBy("sector")
    .toPandas()
)

fig = px.bar(
    sector_pdf, x="sector", y="avg_return_pct",
    color="avg_return_pct", color_continuous_scale="RdYlGn",
    title="Average Sector Return %",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation Highlights
# MAGIC
# MAGIC Most and least correlated stock pairs.

# COMMAND ----------

top_corr = corr_df.orderBy(F.desc("correlation")).limit(10)
low_corr = corr_df.orderBy(F.asc("correlation")).limit(10)

print("Most correlated pairs:")
display(top_corr)

print("Least correlated pairs:")
display(low_corr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation Heatmap

# COMMAND ----------

import plotly.express as px
import pandas as pd

corr_pdf = corr_df.toPandas()
symbols = sorted(set(corr_pdf["symbol_a"]) | set(corr_pdf["symbol_b"]))
matrix = pd.DataFrame(index=symbols, columns=symbols, dtype=float)
for _, row in corr_pdf.iterrows():
    matrix.loc[row["symbol_a"], row["symbol_b"]] = row["correlation"]
    matrix.loc[row["symbol_b"], row["symbol_a"]] = row["correlation"]
for s in symbols:
    matrix.loc[s, s] = 1.0

fig2 = px.imshow(
    matrix.astype(float),
    color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
    title="Stock Correlation Heatmap",
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Insights Summary

# COMMAND ----------

insights = latest.join(fundamentals_df, on="symbol", how="left").select(
    "symbol", "close", "daily_return_pct", "rsi_14",
    "sma_20", "sma_50", "signals",
    "pe_ratio", "forward_pe", "market_cap", "dividend_yield",
)

insights.coalesce(1).write.mode("overwrite").parquet(
    f"{GOLD_ROOT}/insights_export"
)
print("Insights exported to gold/insights_export")
