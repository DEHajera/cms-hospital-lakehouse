# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold: hospital quality scorecard
# MAGIC
# MAGIC Denormalized, BI-ready table with one row per (hospital_id, year).
# MAGIC
# MAGIC **Columns:**
# MAGIC - Identity: `hospital_id`, `hospital_name`, `state`, `hospital_type`
# MAGIC - Quality: `overall_rating`, `readmission_rate`, `excess_readmission_ratio`
# MAGIC - Experience: `hcahps_communication_topbox`, `hcahps_cleanliness_topbox`, `hcahps_overall_topbox`
# MAGIC - Care: `ed_wait_time_minutes`, `sepsis_care_pct`
# MAGIC - Audit: `_gold_ts`, `_source_batch_id`
# MAGIC
# MAGIC **Partition:** `state`. **Z-order:** `(year, hospital_id)`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver

# COMMAND ----------

hospital = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_hospital")
# TODO: uncomment once Silver #2-#4 are implemented
# readm    = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_readmission_measure")
# hcahps   = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_patient_experience")
# care     = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_care_measure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the scorecard
# MAGIC
# MAGIC The join pattern below is deliberately written so it's easy to reason about — joins are left-anchored on `hospital`, and each measure pivots its rows into named columns using conditional aggregations. This avoids a correlated subquery per measure and keeps the final shape analyst-friendly.

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** implement the scorecard.
# MAGIC
# MAGIC Shape target:
# MAGIC ```
# MAGIC hospital_id | hospital_name | state | hospital_type | year |
# MAGIC   overall_rating | readmission_rate_overall | excess_readmission_ratio_heart_attack |
# MAGIC   hcahps_communication_topbox | hcahps_cleanliness_topbox | hcahps_overall_topbox |
# MAGIC   ed_wait_time_minutes | sepsis_care_pct |
# MAGIC   _gold_ts | _source_batch_id
# MAGIC ```
# MAGIC
# MAGIC Implementation hints:
# MAGIC 1. Start from `hospital` (one row per hospital).
# MAGIC 2. Derive `year` from the latest Silver batch date.
# MAGIC 3. Pivot readmission measures: `F.avg(F.when(F.col("measure_id") == "READM_30_HOSP_WIDE", F.col("rate"))).alias("readmission_rate_overall")`.
# MAGIC 4. Similarly pivot HCAHPS top-box scores by measure name.
# MAGIC 5. Similarly pivot Care measures.
# MAGIC 6. Left-join measure pivots onto hospital.
# MAGIC 7. Add `_gold_ts` and `_source_batch_id` columns.
# MAGIC 8. Write as a new Gold Delta table.
# MAGIC
# MAGIC Sample skeleton:

# COMMAND ----------

# Placeholder skeleton — replace with your implementation
scorecard = (hospital
    .withColumn("year", F.year(F.col("_ingest_ts")))
    .select(
        "hospital_id", "hospital_name", "state", "hospital_type",
        "overall_rating", "year",
        F.lit(None).cast("decimal(6,4)").alias("readmission_rate_overall"),
        F.lit(None).cast("decimal(6,4)").alias("excess_readmission_ratio_heart_attack"),
        F.lit(None).cast("decimal(5,2)").alias("hcahps_communication_topbox"),
        F.lit(None).cast("decimal(5,2)").alias("hcahps_cleanliness_topbox"),
        F.lit(None).cast("decimal(5,2)").alias("hcahps_overall_topbox"),
        F.lit(None).cast("int").alias("ed_wait_time_minutes"),
        F.lit(None).cast("decimal(5,2)").alias("sepsis_care_pct"),
    )
    .withColumn("_gold_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_gold_ts").cast("string"))
)

target = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard"

if spark.catalog.tableExists(target):
    # Overwrite for simplicity on a learning project; in production this would be MERGE or a partitioned write.
    (scorecard.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .partitionBy("state")
        .saveAsTable(target))
else:
    (scorecard.write
        .format("delta")
        .partitionBy("state")
        .saveAsTable(target))

print(f"✓ Wrote {target} with {scorecard.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize for the dashboard access pattern

# COMMAND ----------

spark.sql(f"OPTIMIZE {CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard ZORDER BY (year, hospital_id)")
print("✓ OPTIMIZE ZORDER complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, COUNT(*) n_hospitals, AVG(overall_rating) avg_rating
# MAGIC FROM hajera_lakehouse_gold.gold_hospital_scorecard
# MAGIC GROUP BY state
# MAGIC ORDER BY n_hospitals DESC
# MAGIC LIMIT 15
