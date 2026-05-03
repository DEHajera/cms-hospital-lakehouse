# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: clean, type, dedupe, MERGE
# MAGIC
# MAGIC Turns raw Bronze into trustworthy Silver.
# MAGIC
# MAGIC **For each source table:**
# MAGIC - Cast columns to strict types (e.g., `hospital_id STRING`, dates as `DATE`, scores as `DECIMAL`).
# MAGIC - Trim/upper-case standardizations (state codes, facility names).
# MAGIC - Deduplicate by `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY _ingest_ts DESC)`.
# MAGIC - Write with `MERGE INTO` (upsert pattern — safe to re-run).
# MAGIC - Partition by `state`.
# MAGIC
# MAGIC **Inputs:** 4 Bronze tables.
# MAGIC **Outputs:** 4 Silver tables.
# MAGIC **Runtime:** ~2 minutes.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType, DateType, DecimalType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: generic Silver writer
# MAGIC
# MAGIC Wraps the `MERGE INTO` pattern so every Silver table uses the same idempotent write semantics.

# COMMAND ----------

def merge_to_silver(df, target_table: str, key_cols: list):
    
    """
    Upsert a DataFrame into a liquid-clustered Silver Delta table.
    Creates the table on first run with CLUSTER BY (state, provider_id)
    when both columns exist; MERGEs on subsequent runs.
    """

    full_target = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{target_table}"

    df = df.withColumn("_silver_ts", F.current_timestamp())

    if not spark.catalog.tableExists(full_target):
        writer = df.write.format("delta")
        # Liquid clustering on (state, <business_key>) — see ARCHITECTURE.md
        # Accepts either provider_id or hospital_id as the second clustering key.
        second_key = next(
            (k for k in ["provider_id", "hospital_id"] if k in df.columns),
            None
        )
        if second_key and "state" in df.columns:
            writer = writer.clusterBy("state", second_key)
        elif "state" in df.columns:
            writer = writer.clusterBy("state")
        writer.saveAsTable(full_target)
        print(f"  ✓ Created {full_target} with {df.count():,} rows (clustered)")
        return

    df.createOrReplaceTempView("_silver_source")
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
    spark.sql(f"""
        MERGE INTO {full_target} t
        USING _silver_source s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  ✓ Merged into {full_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver #1 — `hospital`
# MAGIC
# MAGIC Master hospital dimension. One row per Facility ID.

# COMMAND ----------

spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_hospital_general").printSchema()

# COMMAND ----------

bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_hospital_general")

# Defensive column renaming — CMS columns have spaces and caps; normalize to snake_case
def snake(col):
    return (col.strip().lower()
            .replace(" ", "_").replace("-", "_").replace("/", "_")
            .replace("(", "").replace(")", "").replace(",", ""))

for orig in bronze.columns:
    new = snake(orig)
    if new != orig:
        bronze = bronze.withColumnRenamed(orig, new)

# Dedupe — keep latest per facility_id
w = Window.partitionBy("facility_id").orderBy(F.col("_ingest_ts").desc())
bronze_deduped = (bronze
                  .withColumn("_rn", F.row_number().over(w))
                  .where(F.col("_rn") == 1)
                  .drop("_rn"))

silver_hospital = (bronze_deduped
    .select(
        F.col("facility_id").cast(IntegerType()).cast(StringType()).alias("hospital_id"),
        F.trim(F.col("facility_name")).alias("hospital_name"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        F.trim(F.col("city_town")).alias("city"),
        F.trim(F.col("address")).alias("address"),
        F.col("zip_code").cast(StringType()).alias("zip_code"),
        F.trim(F.col("hospital_type")).alias("hospital_type"),
        F.trim(F.col("hospital_ownership")).alias("hospital_ownership"),
        F.trim(F.col("county_parish")).alias("county"),
        F.trim(F.col("telephone_number")).alias("phone"),
        (F.upper(F.trim(F.col("emergency_services"))) == "YES").alias("has_emergency_services"),
        F.col("hospital_overall_rating").isin("Not Available", "N/A", "*").alias("is_overall_rating_suppressed"),
        F.when(
            F.col("hospital_overall_rating").isin("Not Available", "N/A", "*"),
            None
        ).otherwise(F.col("hospital_overall_rating").cast(IntegerType())).alias("overall_rating"),
        F.col("_ingest_ts"),
        F.col("_batch_id"),
    )
    .where(F.col("hospital_id").isNotNull())
    .where(F.col("state").isNotNull())
)

merge_to_silver(silver_hospital, "silver_hospital", ["hospital_id"])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT hospital_id) AS unique_hospitals
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT hospital_id) AS unique_hospitals
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   hospital_id, 
# MAGIC   hospital_name, 
# MAGIC   state, 
# MAGIC   city, 
# MAGIC   hospital_type,
# MAGIC   is_overall_rating_suppressed, 
# MAGIC   overall_rating
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC WHERE state = 'VA'
# MAGIC ORDER BY overall_rating DESC NULLS LAST
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_readmissions").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver #2 — `readmission_measure`
# MAGIC
# MAGIC One row per (hospital_id, measure_id, start_date).

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** Implement the same pattern as Silver #1 for the readmissions Bronze table.
# MAGIC
# MAGIC Key transforms:
# MAGIC - Rename Bronze columns to snake_case (reuse the `snake()` helper above — consider moving it to a common util cell at the top of the notebook).
# MAGIC - Cast `excess_readmission_ratio` and `predicted_readmission_rate` to `DECIMAL(5,4)`.
# MAGIC - Cast `start_date`, `end_date` to `DATE`.
# MAGIC - Join `hospital_id` to `silver_hospital` to attach `state` (needed for partitioning).
# MAGIC - Dedupe on (hospital_id, measure_id, start_date).
# MAGIC - `merge_to_silver(df, "silver_readmission_measure", ["hospital_id", "measure_id", "start_date"])`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver #3 — `patient_experience`
# MAGIC
# MAGIC HCAHPS patient-experience scores — communication, responsiveness, cleanliness, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** Implement Silver #3 following the pattern above.
# MAGIC
# MAGIC Notes specific to HCAHPS:
# MAGIC - HCAHPS has a "top-box" / "bottom-box" structure; parse the relevant columns as percentages (`DECIMAL(5,2)`).
# MAGIC - Measure IDs are in `hcahps_measure_id`.
# MAGIC - Dedupe on (hospital_id, hcahps_measure_id).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver #4 — `care_measure`
# MAGIC
# MAGIC Timely and Effective Care metrics (ER wait, sepsis care, etc.).

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** Implement Silver #4 following the pattern above. Dedupe on (hospital_id, measure_id).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-write maintenance
# MAGIC
# MAGIC Run `OPTIMIZE` to compact small files, Z-order for the access pattern, and `VACUUM` to reclaim space.
# MAGIC
# MAGIC **DBA callout:** this is the lakehouse equivalent of an index rebuild + log cleanup.

# COMMAND ----------

for tbl in ["silver_hospital", "silver_readmission_measure", "silver_patient_experience", "silver_care_measure"]:
    full = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{tbl}"
    if spark.catalog.tableExists(full):
        try:
            spark.sql(f"OPTIMIZE {full} ZORDER BY (_silver_ts)")
            print(f"  ✓ Optimized {tbl}")
        except Exception as e:
            print(f"  - Skipped optimize for {tbl}: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS hospitals, COUNT(DISTINCT state) AS n_states FROM hajera_lakehouse_silver.silver_hospital
