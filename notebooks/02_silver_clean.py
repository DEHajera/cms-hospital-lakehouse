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

# ─── Silver #2 — readmission_measure ────────────────────────────────────────────
# One row per (hospital_id, measure_name, start_date).
# Pattern follows Silver #1: defensive type contracts, suppression-aware casts,
# multi-column dedupe via Window, MERGE INTO via merge_to_silver helper.

bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_readmissions")

# Reuse the snake() helper defined in Silver #1 to normalize column names
for orig in bronze.columns:
    new = snake(orig)
    if new != orig:
        bronze = bronze.withColumnRenamed(orig, new)

# Sentinel values CMS uses to suppress small-cell measures
SUPPRESSED = ["Not Available", "Not Applicable", "Too Few Cases", "Too Few to Report", "Number too small to report", "*", "N/A"]

# Dedupe — keep latest per (facility_id, measure_name, start_date)
w = (Window
     .partitionBy("facility_id", "measure_name", "start_date")
     .orderBy(F.col("_ingest_ts").desc()))
bronze_deduped = (bronze
                  .withColumn("_rn", F.row_number().over(w))
                  .where(F.col("_rn") == 1)
                  .drop("_rn"))

# Project Silver schema
silver_readmission = (bronze_deduped
    .select(
        # Business keys
        F.col("facility_id").cast(IntegerType()).cast(StringType()).alias("hospital_id"),
        F.trim(F.col("measure_name")).alias("measure_name"),
        F.to_date(F.col("start_date"), "MM/dd/yyyy").alias("start_date"),
        F.col("end_date").alias("end_date"),

        # Hospital context (denormalized for query convenience)
        F.trim(F.col("facility_name")).alias("hospital_name"),
        F.upper(F.trim(F.col("state"))).alias("state"),

        # Numeric measures — suppression-aware casts
        F.col("number_of_discharges").isin(SUPPRESSED).alias("is_discharges_suppressed"),
        F.when(F.col("number_of_discharges").isin(SUPPRESSED), None)
         .otherwise(F.col("number_of_discharges").cast(IntegerType()))
         .alias("discharges"),

        F.col("excess_readmission_ratio").isin(SUPPRESSED).alias("is_excess_ratio_suppressed"),
        F.when(F.col("excess_readmission_ratio").isin(SUPPRESSED), None)
         .otherwise(F.col("excess_readmission_ratio").cast(DecimalType(5, 4)))
         .alias("excess_readmission_ratio"),

        F.col("predicted_readmission_rate").isin(SUPPRESSED).alias("is_predicted_rate_suppressed"),
        F.when(F.col("predicted_readmission_rate").isin(SUPPRESSED), None)
         .otherwise(F.col("predicted_readmission_rate").cast(DecimalType(6, 4)))
         .alias("predicted_readmission_rate"),

        F.col("expected_readmission_rate").isin(SUPPRESSED).alias("is_expected_rate_suppressed"),
        F.when(F.col("expected_readmission_rate").isin(SUPPRESSED), None)
         .otherwise(F.col("expected_readmission_rate").cast(DecimalType(6, 4)))
         .alias("expected_readmission_rate"),

        F.col("number_of_readmissions").isin(SUPPRESSED).alias("is_readmissions_suppressed"),
        F.when(F.col("number_of_readmissions").isin(SUPPRESSED), None)
         .otherwise(F.col("number_of_readmissions").cast(IntegerType()))
         .alias("readmissions"),

        # Audit columns from Bronze
        F.col("_ingest_ts"),
        F.col("_batch_id"),
    )
    .where(F.col("hospital_id").isNotNull())
    .where(F.col("measure_name").isNotNull())
    .where(F.col("start_date").isNotNull())
)

merge_to_silver(
    silver_readmission,
    "silver_readmission_measure",
    ["hospital_id", "measure_name", "start_date"]
)

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

# ─── Silver #3 — patient_experience (HCAHPS) ────────────────────────────────────
# One row per (hospital_id, hcahps_measure_id, hcahps_answer_description, start_date).
# Pattern follows Silver #1/#2: defensive type contracts, suppression-aware casts,
# multi-column dedupe via Window, MERGE INTO via merge_to_silver helper.

bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_hcahps")

# Reuse the snake() helper defined in Silver #1 to normalize column names
for orig in bronze.columns:
    new = snake(orig)
    if new != orig:
        bronze = bronze.withColumnRenamed(orig, new)

# Dedupe — keep latest per composite key
w = (Window
     .partitionBy("facility_id", "hcahps_measure_id", "hcahps_answer_description", "start_date")
     .orderBy(F.col("_ingest_ts").desc()))
bronze_deduped = (bronze
                  .withColumn("_rn", F.row_number().over(w))
                  .where(F.col("_rn") == 1)
                  .drop("_rn"))

# Project Silver schema
silver_patient_experience = (bronze_deduped
    .select(
        # Business keys
        F.col("facility_id").cast(IntegerType()).cast(StringType()).alias("hospital_id"),
        F.trim(F.col("hcahps_measure_id")).alias("hcahps_measure_id"),
        F.trim(F.col("hcahps_answer_description")).alias("hcahps_answer_description"),
        F.to_date(F.col("start_date"), "MM/dd/yyyy").alias("start_date"),
        F.col("end_date").alias("end_date"),

        # Measure context
        F.trim(F.col("hcahps_question")).alias("hcahps_question"),

        # Hospital context (denormalized for query convenience)
        F.trim(F.col("facility_name")).alias("hospital_name"),
        F.upper(F.trim(F.col("state"))).alias("state"),

        # Numeric measures — suppression-aware casts
        F.col("patient_survey_star_rating").isin(SUPPRESSED).alias("is_star_rating_suppressed"),
        F.when(F.col("patient_survey_star_rating").isin(SUPPRESSED), None)
         .otherwise(F.col("patient_survey_star_rating").cast(IntegerType()))
         .alias("star_rating"),

        F.col("hcahps_answer_percent").isin(SUPPRESSED).alias("is_answer_percent_suppressed"),
        F.when(F.col("hcahps_answer_percent").isin(SUPPRESSED), None)
         .otherwise(F.col("hcahps_answer_percent").cast(DecimalType(5, 2)))
         .alias("answer_percent"),

        F.col("hcahps_linear_mean_value").isin(SUPPRESSED).alias("is_linear_mean_suppressed"),
        F.when(F.col("hcahps_linear_mean_value").isin(SUPPRESSED), None)
         .otherwise(F.col("hcahps_linear_mean_value").cast(DecimalType(5, 2)))
         .alias("linear_mean_value"),

        F.col("number_of_completed_surveys").isin(SUPPRESSED).alias("is_survey_count_suppressed"),
        F.when(F.col("number_of_completed_surveys").isin(SUPPRESSED), None)
         .otherwise(F.col("number_of_completed_surveys").cast(IntegerType()))
         .alias("completed_surveys"),

        F.col("survey_response_rate_percent").isin(SUPPRESSED).alias("is_response_rate_suppressed"),
        F.when(F.col("survey_response_rate_percent").isin(SUPPRESSED), None)
         .otherwise(F.col("survey_response_rate_percent").cast(DecimalType(5, 2)))
         .alias("response_rate_percent"),

        # Transparency: keep the footnote text where present (helps Gold-layer narrative)
        F.col("hcahps_answer_percent_footnote").alias("answer_percent_footnote"),

        # Audit columns from Bronze
        F.col("_ingest_ts"),
        F.col("_batch_id"),
    )
    .where(F.col("hospital_id").isNotNull())
    .where(F.col("hcahps_measure_id").isNotNull())
    .where(F.col("hcahps_answer_description").isNotNull())
    .where(F.col("start_date").isNotNull())
)

merge_to_silver(
    silver_patient_experience,
    "silver_patient_experience",
    ["hospital_id", "hcahps_measure_id", "hcahps_answer_description", "start_date"]
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT CONCAT(
# MAGIC     hospital_id, '|', 
# MAGIC     hcahps_measure_id, '|', 
# MAGIC     hcahps_answer_description, '|', 
# MAGIC     CAST(start_date AS STRING)
# MAGIC   )) AS unique_composite_keys
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_patient_experience;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver #4 — `care_measure`
# MAGIC
# MAGIC Timely and Effective Care metrics (ER wait, sepsis care, etc.).

# COMMAND ----------

spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_timely_care").printSchema()

# COMMAND ----------

# ─── Silver #4 — care_measure (Timely and Effective Care) ──────────────────────
# One row per (hospital_id, measure_id, start_date).
# Pattern follows Silver #1/#2/#3: defensive type contracts, suppression-aware
# casts, multi-column dedupe via Window, MERGE INTO via merge_to_silver helper.

bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_timely_care")

# Reuse the snake() helper to normalize column names
for orig in bronze.columns:
    new = snake(orig)
    if new != orig:
        bronze = bronze.withColumnRenamed(orig, new)

# Dedupe — keep latest per (facility_id, measure_id, start_date)
w = (Window
     .partitionBy("facility_id", "measure_id", "start_date")
     .orderBy(F.col("_ingest_ts").desc()))
bronze_deduped = (bronze
                  .withColumn("_rn", F.row_number().over(w))
                  .where(F.col("_rn") == 1)
                  .drop("_rn"))

# Project Silver schema
silver_care_measure = (bronze_deduped
    .select(
        # Business keys
        F.col("facility_id").cast(IntegerType()).cast(StringType()).alias("hospital_id"),
        F.trim(F.col("measure_id")).alias("measure_id"),
        F.to_date(F.col("start_date"), "MM/dd/yyyy").alias("start_date"),
        F.col("end_date").alias("end_date"),

        # Measure context
        F.trim(F.col("measure_name")).alias("measure_name"),
        F.trim(F.col("condition")).alias("condition"),

        # Hospital context (denormalized for query convenience)
        F.trim(F.col("facility_name")).alias("hospital_name"),
        F.upper(F.trim(F.col("state"))).alias("state"),

        # Numeric measures — suppression-aware casts
        # Note: Score is stored as string because some measures report integers
        # (e.g., median minutes), others report percentages. Keep as DECIMAL(7,2)
        # to accommodate both safely.
        # Score column has three flavors in CMS timely-care:
        #   (1) Numeric scores (percentages, minutes) — cast to DECIMAL
        #   (2) Categorical scores (low/medium/high for some sepsis measures) — keep as string
        #   (3) Suppressed sentinels (Not Available, Too Few Cases, etc.) — null both
        F.col("score").isin(SUPPRESSED).alias("is_score_suppressed"),
        # Numeric score — populated only when value parses as a number
        F.when(F.col("score").isin(SUPPRESSED), None)
         .when(F.lower(F.trim(F.col("score"))).isin("low", "medium", "high"), None)
         .otherwise(F.expr("try_cast(score AS DECIMAL(7,2))"))
         .alias("score_numeric"),
        # Categorical score — populated only for ordinal values
        F.when(F.lower(F.trim(F.col("score"))).isin("low", "medium", "high"),
               F.lower(F.trim(F.col("score"))))
         .otherwise(None)
         .alias("score_category"),

        F.col("sample").isin(SUPPRESSED).alias("is_sample_suppressed"),
        F.when(F.col("sample").isin(SUPPRESSED), None)
         .otherwise(F.expr("try_cast(sample AS INT)"))
         .alias("sample_size"),

        # Transparency: keep footnote where present
        F.col("footnote").alias("footnote"),

        # Audit columns from Bronze
        F.col("_ingest_ts"),
        F.col("_batch_id"),
    )
    .where(F.col("hospital_id").isNotNull())
    .where(F.col("measure_id").isNotNull())
    .where(F.col("start_date").isNotNull())
)

merge_to_silver(
    silver_care_measure,
    "silver_care_measure",
    ["hospital_id", "measure_id", "start_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO (Hajera):** Implement Silver #4 following the pattern above. Dedupe on (hospital_id, measure_id).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT CONCAT(hospital_id, '|', measure_id, '|', CAST(start_date AS STRING))) AS unique_keys
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure;

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_silver_ts), MAX(_silver_ts) 
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify row count and uniqueness on composite key
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT CONCAT(hospital_id, '|', measure_name, '|', CAST(start_date AS STRING))) AS unique_composite_keys
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify clustering is active
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   clusteringColumns,
# MAGIC   partitionColumns,
# MAGIC   numFiles,
# MAGIC   sizeInBytes
# MAGIC FROM (DESCRIBE DETAIL workspace.hajera_lakehouse_silver.silver_readmission_measure);

# COMMAND ----------

spark.sql("DESCRIBE DETAIL workspace.hajera_lakehouse_silver.silver_readmission_measure") \
     .select("name", "clusteringColumns", "partitionColumns", "numFiles", "sizeInBytes") \
     .show(truncate=False)

# COMMAND ----------

spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_hcahps").printSchema()
