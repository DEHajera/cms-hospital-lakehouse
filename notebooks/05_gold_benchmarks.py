# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold: peer benchmarks
# MAGIC
# MAGIC Percentile-rank each hospital against its peer group.
# MAGIC
# MAGIC A "peer group" = `(state, hospital_type, bed_count_band)`. This matters because ranking a 40-bed rural critical-access hospital against Johns Hopkins is statistically meaningless. The scorecard compares peers to peers.
# MAGIC
# MAGIC **Output:** `gold.gold_peer_benchmarks` — one row per hospital per metric with a 0–100 percentile value.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F, Window

scorecard = spark.read.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build peer benchmarks

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** implement peer benchmarking.
# MAGIC
# MAGIC Implementation sketch:
# MAGIC
# MAGIC 1. Define the peer-group partitioning:
# MAGIC    ```
# MAGIC    peer_win = Window.partitionBy("state", "hospital_type")
# MAGIC    ```
# MAGIC    (Once you have bed counts, add a `bed_count_band` column and include it.)
# MAGIC
# MAGIC 2. For each metric, compute `percent_rank()` within the peer group:
# MAGIC    ```
# MAGIC    .withColumn(
# MAGIC      "readmission_rate_pctile",
# MAGIC      (F.percent_rank().over(peer_win.orderBy(F.col("readmission_rate_overall").desc_nulls_last())) * 100)
# MAGIC    )
# MAGIC    ```
# MAGIC    NOTE on direction: lower readmission rate = better, so order **descending** to make a low rate = high percentile.
# MAGIC
# MAGIC 3. Pivot into a long format (hospital_id | metric | value | peer_percentile | peer_group_size) — this shape is easier for the dashboard to consume than a wide one for this data.
# MAGIC
# MAGIC 4. Write to `gold.gold_peer_benchmarks`; partition by state.

# COMMAND ----------

# Placeholder skeleton
peer_win = Window.partitionBy("state", "hospital_type")

benchmarks = (scorecard
    .withColumn(
        "overall_rating_pctile",
        F.percent_rank().over(peer_win.orderBy(F.col("overall_rating").asc_nulls_last())) * 100
    )
    .withColumn("peer_group_size", F.count("*").over(peer_win))
)

target = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_peer_benchmarks"
(benchmarks.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .partitionBy("state")
    .saveAsTable(target))

print(f"✓ Wrote {target} ({benchmarks.count():,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, hospital_type, ROUND(AVG(overall_rating_pctile),1) avg_pctile, AVG(peer_group_size) peer_size
# MAGIC FROM hajera_lakehouse_gold.gold_peer_benchmarks
# MAGIC GROUP BY state, hospital_type
# MAGIC ORDER BY peer_size DESC
# MAGIC LIMIT 20
