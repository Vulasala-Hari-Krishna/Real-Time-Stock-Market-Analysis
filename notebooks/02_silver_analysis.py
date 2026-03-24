# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer Analysis
# MAGIC
# MAGIC Read the cleaned **silver** Parquet data, compute technical indicators, and
# MAGIC produce interactive visualisations of price trends and distributions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BUCKET = spark.conf.get("spark.stock.s3_bucket", "stock-market-datalake")
SILVER_PATH = f"s3a://{BUCKET}/silver/historical"

FOCUS_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "NVDA"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Parquet

# COMMAND ----------

silver_df = spark.read.parquet(SILVER_PATH)
print(f"Total rows: {silver_df.count()}")
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter to Focus Symbols

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

focus_df = silver_df.filter(F.col("symbol").isin(FOCUS_SYMBOLS))
print(f"Filtered rows: {focus_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Simple Moving Averages

# COMMAND ----------

window_spec = Window.partitionBy("symbol").orderBy("date")

for period in [20, 50]:
    focus_df = focus_df.withColumn(
        f"sma_{period}",
        F.avg("close").over(window_spec.rowsBetween(-period + 1, 0)),
    )

display(focus_df.filter(F.col("symbol") == "AAPL").orderBy(F.desc("date")).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Returns Distribution

# COMMAND ----------

returns_df = focus_df.withColumn(
    "prev_close",
    F.lag("close", 1).over(window_spec),
).withColumn(
    "daily_return_pct",
    ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100,
)

display(
    returns_df.groupBy("symbol")
    .agg(
        F.mean("daily_return_pct").alias("avg_return"),
        F.stddev("daily_return_pct").alias("std_return"),
        F.min("daily_return_pct").alias("min_return"),
        F.max("daily_return_pct").alias("max_return"),
    )
    .orderBy("symbol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Trend Visualisation
# MAGIC
# MAGIC Convert to Pandas for Plotly charts (small dataset after filtering).

# COMMAND ----------

import plotly.express as px

pdf = (
    focus_df.select("date", "symbol", "close", "sma_20", "sma_50")
    .orderBy("date")
    .toPandas()
)

fig = px.line(
    pdf, x="date", y="close", color="symbol",
    title="Closing Price Trends",
    labels={"close": "Close ($)", "date": "Date"},
)
fig.update_layout(height=450)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SMA Overlay for AAPL

# COMMAND ----------

import plotly.graph_objects as go

aapl = pdf[pdf["symbol"] == "AAPL"]
fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=aapl["date"], y=aapl["close"], name="Close"))
fig2.add_trace(go.Scatter(x=aapl["date"], y=aapl["sma_20"], name="SMA 20", line={"dash": "dot"}))
fig2.add_trace(go.Scatter(x=aapl["date"], y=aapl["sma_50"], name="SMA 50", line={"dash": "dash"}))
fig2.update_layout(title="AAPL Price & Moving Averages", height=400)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Analysis

# COMMAND ----------

vol_pdf = (
    focus_df.select("date", "symbol", "volume")
    .orderBy("date")
    .toPandas()
)

fig3 = px.bar(
    vol_pdf, x="date", y="volume", color="symbol",
    title="Daily Trading Volume",
    barmode="group",
)
fig3.update_layout(height=400)
fig3.show()
