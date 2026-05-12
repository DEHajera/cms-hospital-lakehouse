# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver data-quality checks
# MAGIC
# MAGIC Every Silver table is validated before anything downstream is allowed to consume it.
# MAGIC
# MAGIC **Check types:**
# MAGIC | Type | Severity |
# MAGIC |---|---|
# MAGIC | Schema contract (primary-key non-null + unique) | HARD — raises |
# MAGIC | Null-rate threshold | SOFT — logs |
# MAGIC | Range validation | SOFT — logs |
# MAGIC | Referential integrity | HARD — raises |
# MAGIC | Freshness (newest row within N days) | WARNING |
# MAGIC | Row-count volume (±10% vs prior batch) | WARNING |
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `silver.dq_run_summary` — one row per (batch_id, table, check, result).
# MAGIC - `silver.dq_failed_rows` — the actual failing rows, for debugging.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
run_ts = F.current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ result recorder
# MAGIC
# MAGIC One place where all results are written — easy to later swap for Great Expectations or Deequ.

# COMMAND ----------

dq_results = []

def record(table, check, severity, result, rows_failed=0, detail=""):
    dq_results.append({
        "batch_id": BATCH_ID,
        "table": table,
        "check": check,
        "severity": severity,
        "result": result,                # "pass" | "fail" | "warn"
        "rows_failed": int(rows_failed),
        "detail": detail[:500],
    })
    icon = {"pass": "✓", "fail": "✗", "warn": "!"}.get(result, "?")
    print(f"  {icon} [{severity}] {table}.{check}: {result} ({rows_failed} failing) — {detail[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable check functions

# COMMAND ----------

def check_pk_not_null_unique(df, table, key_col):
    nulls = df.where(F.col(key_col).isNull()).count()
    if nulls > 0:
        record(table, f"pk_not_null({key_col})", "HARD", "fail", nulls,
               f"{nulls} null values in PK {key_col}")
        return False
    dupes = (df.groupBy(key_col).count().where(F.col("count") > 1).count())
    if dupes > 0:
        record(table, f"pk_unique({key_col})", "HARD", "fail", dupes,
               f"{dupes} duplicate {key_col} values")
        return False
    record(table, f"pk({key_col})", "HARD", "pass")
    return True

def check_null_rate(df, table, col, max_rate):
    total = df.count()
    if total == 0:
        record(table, f"null_rate({col})", "SOFT", "warn", 0, "empty table")
        return
    nulls = df.where(F.col(col).isNull()).count()
    rate = nulls / total
    if rate > max_rate:
        record(table, f"null_rate({col})", "SOFT", "fail", nulls,
               f"null rate {rate:.1%} > threshold {max_rate:.1%}")
    else:
        record(table, f"null_rate({col})", "SOFT", "pass", nulls,
               f"null rate {rate:.1%}")

def check_range(df, table, col, min_v, max_v):
    failing = df.where((F.col(col) < min_v) | (F.col(col) > max_v)).count()
    if failing > 0:
        record(table, f"range({col},{min_v}..{max_v})", "SOFT", "fail", failing,
               f"{failing} rows outside [{min_v},{max_v}]")
    else:
        record(table, f"range({col},{min_v}..{max_v})", "SOFT", "pass")

def check_referential(parent_df, child_df, parent_key, child_key, child_table):
    parent_keys = parent_df.select(parent_key).distinct()
    orphans = (child_df.join(parent_keys,
                             child_df[child_key] == parent_keys[parent_key],
                             "left_anti").count())
    if orphans > 0:
        record(child_table, f"fk({child_key})", "HARD", "fail", orphans,
               f"{orphans} rows reference missing parent")
        return False
    record(child_table, f"fk({child_key})", "HARD", "pass")
    return True

def check_freshness(df, table, ts_col, max_age_days):
    newest = df.agg(F.max(ts_col)).collect()[0][0]
    if newest is None:
        record(table, f"freshness({ts_col})", "WARN", "warn", 0, "no rows")
        return
    age = (datetime.now(timezone.utc) - newest.replace(tzinfo=timezone.utc)).days
    if age > max_age_days:
        record(table, f"freshness({ts_col})", "WARN", "warn", 0,
               f"newest row is {age}d old (> {max_age_days}d)")
    else:
        record(table, f"freshness({ts_col})", "WARN", "pass", 0, f"{age}d old")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run checks

# COMMAND ----------

hospital = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_hospital")

# Schema contract
check_pk_not_null_unique(hospital, "silver_hospital", "hospital_id")

# Null-rate checks
check_null_rate(hospital, "silver_hospital", "state", max_rate=0.0)
check_null_rate(hospital, "silver_hospital", "hospital_name", max_rate=0.01)
check_null_rate(hospital, "silver_hospital", "overall_rating", max_rate=0.25)  # CMS reports many as "Not Available"

# Range
check_range(hospital, "silver_hospital", "overall_rating", 1, 5)

# Freshness
check_freshness(hospital, "silver_hospital", "_ingest_ts", max_age_days=180)

# TODO: repeat the appropriate subset of checks for:
#   silver_readmission_measure
#   silver_patient_experience
#   silver_care_measure
# And add referential-integrity checks: every child.hospital_id must exist in silver_hospital.
# ─── silver_readmission_measure ────────────────────────────────────────────────
readmission = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_readmission_measure")

# PK check on composite key — null+unique check on a synthesized concatenation
readmission_pk = readmission.withColumn(
    "_pk", F.concat_ws("|", F.col("hospital_id"), F.col("measure_name"), F.col("start_date").cast("string"))
)
check_pk_not_null_unique(readmission_pk, "silver_readmission_measure", "_pk")

# Null-rate checks — business keys must never be null
check_null_rate(readmission, "silver_readmission_measure", "hospital_id", max_rate=0.0)
check_null_rate(readmission, "silver_readmission_measure", "measure_name", max_rate=0.0)
check_null_rate(readmission, "silver_readmission_measure", "start_date", max_rate=0.0)

# Range — excess readmission ratio is centered on 1.0; CMS reports values 0.5–2.0 typically
check_range(readmission, "silver_readmission_measure", "excess_readmission_ratio", 0, 10)

# Freshness — CMS Care Compare refreshes quarterly, so 180 days is the tolerance
check_freshness(readmission, "silver_readmission_measure", "_ingest_ts", max_age_days=180)


# ─── silver_patient_experience (HCAHPS) ────────────────────────────────────────
patient_experience = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_patient_experience")

# PK check on 4-column composite
pe_pk = patient_experience.withColumn(
    "_pk", F.concat_ws("|",
                      F.col("hospital_id"),
                      F.col("hcahps_measure_id"),
                      F.col("hcahps_answer_description"),
                      F.col("start_date").cast("string"))
)
check_pk_not_null_unique(pe_pk, "silver_patient_experience", "_pk")

# Null-rate checks on business keys
check_null_rate(patient_experience, "silver_patient_experience", "hospital_id", max_rate=0.0)
check_null_rate(patient_experience, "silver_patient_experience", "hcahps_measure_id", max_rate=0.0)
check_null_rate(patient_experience, "silver_patient_experience", "hcahps_answer_description", max_rate=0.0)

# Range — answer_percent is a percentage; star_rating is 1–5
check_range(patient_experience, "silver_patient_experience", "answer_percent", 0, 100)
check_range(patient_experience, "silver_patient_experience", "star_rating", 1, 5)

# Freshness
check_freshness(patient_experience, "silver_patient_experience", "_ingest_ts", max_age_days=180)


# ─── silver_care_measure ───────────────────────────────────────────────────────
care_measure = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_care_measure")

# PK check on 3-column composite
cm_pk = care_measure.withColumn(
    "_pk", F.concat_ws("|", F.col("hospital_id"), F.col("measure_id"), F.col("start_date").cast("string"))
)
check_pk_not_null_unique(cm_pk, "silver_care_measure", "_pk")

# Null-rate checks on business keys
check_null_rate(care_measure, "silver_care_measure", "hospital_id", max_rate=0.0)
check_null_rate(care_measure, "silver_care_measure", "measure_id", max_rate=0.0)

# Note: skipping range check on score_numeric — timely-care measures have wildly
# different ranges (minutes for ED-wait, percentages for sepsis-bundle), so a
# single-bound range check is meaningless. A per-measure-id range check would be
# the right shape for a future iteration.

# Freshness
check_freshness(care_measure, "silver_care_measure", "_ingest_ts", max_age_days=180)


# ─── Referential integrity ─────────────────────────────────────────────────────
# Every measure-table hospital_id MUST exist in silver_hospital. Anti-join is
# the scalable way to do this — avoids the gotcha of NOT IN with NULLs.
check_referential(hospital, readmission,         "hospital_id", "hospital_id", "silver_readmission_measure")
check_referential(hospital, patient_experience,  "hospital_id", "hospital_id", "silver_patient_experience")
check_referential(hospital, care_measure,        "hospital_id", "hospital_id", "silver_care_measure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the results

# COMMAND ----------

results_df = spark.createDataFrame(dq_results).withColumn("run_ts", run_ts)
(results_df.write
    .mode("append")
    .format("delta")
    .saveAsTable(f"{CATALOG_NAME}.{SILVER_SCHEMA}.dq_run_summary"))

print(f"\n✓ Appended {results_df.count()} DQ rows to {SILVER_SCHEMA}.dq_run_summary")

# Raise if any HARD failures
hard_fails = [r for r in dq_results if r["severity"] == "HARD" and r["result"] == "fail"]
if hard_fails:
    raise Exception(f"DQ HARD failures blocking Gold build: {len(hard_fails)}")
print("✓ All HARD checks passed — Gold build may proceed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_id, table, check, severity, result, rows_failed
# MAGIC FROM hajera_lakehouse_silver.dq_run_summary
# MAGIC ORDER BY run_ts DESC
# MAGIC LIMIT 50

# COMMAND ----------

print(len(dq_results))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.hajera_lakehouse_silver.silver_hospital;
# MAGIC OPTIMIZE workspace.hajera_lakehouse_silver.silver_readmission_measure;
# MAGIC OPTIMIZE workspace.hajera_lakehouse_silver.silver_patient_experience;
# MAGIC OPTIMIZE workspace.hajera_lakehouse_silver.silver_care_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.hajera_lakehouse_silver.silver_patient_experience LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.hajera_lakehouse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   table,
# MAGIC   COUNT(*) AS total_checks,
# MAGIC   SUM(CASE WHEN result = 'pass' THEN 1 ELSE 0 END) AS passed,
# MAGIC   SUM(CASE WHEN result = 'fail' AND severity = 'HARD' THEN 1 ELSE 0 END) AS hard_fails
# MAGIC FROM workspace.hajera_lakehouse_silver.dq_run_summary
# MAGIC GROUP BY table
# MAGIC ORDER BY table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver_hospital'              AS table, COUNT(*) AS rows FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC UNION ALL
# MAGIC SELECT 'silver_readmission_measure',  COUNT(*) FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC UNION ALL
# MAGIC SELECT 'silver_patient_experience',  COUNT(*) FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC UNION ALL
# MAGIC SELECT 'silver_care_measure',         COUNT(*) FROM workspace.hajera_lakehouse_silver.silver_care_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   table,
# MAGIC   COUNT(*) AS total_checks,
# MAGIC   SUM(CASE WHEN result = 'pass' THEN 1 ELSE 0 END) AS passed,
# MAGIC   SUM(CASE WHEN result = 'fail' AND severity = 'HARD' THEN 1 ELSE 0 END) AS hard_fails
# MAGIC FROM workspace.hajera_lakehouse_silver.dq_run_summary
# MAGIC GROUP BY table
# MAGIC ORDER BY table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1a. Which readmission measures have the broadest hospital coverage?
# MAGIC SELECT 
# MAGIC   measure_name,
# MAGIC   COUNT(DISTINCT hospital_id) AS hospitals_reporting,
# MAGIC   COUNT(*)                    AS total_rows,
# MAGIC   ROUND(AVG(excess_readmission_ratio), 3)        AS avg_ratio,
# MAGIC   ROUND(MIN(excess_readmission_ratio), 3)        AS min_ratio,
# MAGIC   ROUND(MAX(excess_readmission_ratio), 3)        AS max_ratio,
# MAGIC   SUM(CASE WHEN excess_readmission_ratio IS NULL THEN 1 ELSE 0 END) AS null_ratio_rows
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC GROUP BY measure_name
# MAGIC ORDER BY hospitals_reporting DESC;
# MAGIC
# MAGIC -- 1b. Sample one row per measure to eyeball units and semantics
# MAGIC SELECT measure_name, hospital_id, excess_readmission_ratio, discharges, start_date, end_date
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY measure_name ORDER BY hospital_id) AS rn
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC ) WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, data_type, ordinal_position
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'hajera_lakehouse_silver'
# MAGIC ORDER BY table_name, ordinal_position;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1a. Coverage and value distribution per measure_name
# MAGIC SELECT 
# MAGIC   measure_name,
# MAGIC   COUNT(DISTINCT hospital_id)                   AS hospitals_reporting,
# MAGIC   COUNT(*)                                      AS total_rows,
# MAGIC   SUM(CASE WHEN is_excess_ratio_suppressed THEN 1 ELSE 0 END) AS suppressed_rows,
# MAGIC   ROUND(AVG(excess_readmission_ratio), 4)       AS avg_excess_ratio,
# MAGIC   ROUND(MIN(excess_readmission_ratio), 4)       AS min_excess_ratio,
# MAGIC   ROUND(MAX(excess_readmission_ratio), 4)       AS max_excess_ratio,
# MAGIC   ROUND(AVG(predicted_readmission_rate), 2)     AS avg_predicted_pct,
# MAGIC   ROUND(AVG(expected_readmission_rate), 2)      AS avg_expected_pct
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC WHERE is_excess_ratio_suppressed = FALSE
# MAGIC GROUP BY measure_name
# MAGIC ORDER BY hospitals_reporting DESC;
# MAGIC
# MAGIC -- 1b. Discharge volumes per measure (signal for how robust each is)
# MAGIC SELECT 
# MAGIC   measure_name,
# MAGIC   COUNT(*)                AS unsuppressed_rows,
# MAGIC   ROUND(AVG(discharges))  AS avg_discharges_per_hospital,
# MAGIC   SUM(discharges)         AS total_discharges,
# MAGIC   SUM(readmissions)       AS total_readmissions
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC WHERE is_discharges_suppressed = FALSE
# MAGIC GROUP BY measure_name
# MAGIC ORDER BY total_discharges DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2a. Coverage per HCAHPS measure_id (with question text for readability)
# MAGIC SELECT 
# MAGIC   hcahps_measure_id,
# MAGIC   MAX(hcahps_question)                          AS hcahps_question,
# MAGIC   MAX(hcahps_answer_description)                AS answer_description,
# MAGIC   COUNT(DISTINCT hospital_id)                   AS hospitals_reporting,
# MAGIC   COUNT(*)                                      AS total_rows,
# MAGIC   SUM(CASE WHEN is_linear_mean_suppressed = FALSE THEN 1 ELSE 0 END) AS rows_with_linear_mean,
# MAGIC   SUM(CASE WHEN is_answer_percent_suppressed = FALSE THEN 1 ELSE 0 END) AS rows_with_answer_pct,
# MAGIC   SUM(CASE WHEN is_star_rating_suppressed = FALSE THEN 1 ELSE 0 END)   AS rows_with_star
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC GROUP BY hcahps_measure_id
# MAGIC ORDER BY hospitals_reporting DESC, hcahps_measure_id;
# MAGIC
# MAGIC -- 2b. Linear-mean composite values (these are the Gold-grade measures)
# MAGIC SELECT 
# MAGIC   hcahps_measure_id,
# MAGIC   MAX(hcahps_question)              AS hcahps_question,
# MAGIC   COUNT(DISTINCT hospital_id)       AS hospitals,
# MAGIC   ROUND(AVG(linear_mean_value), 2)  AS avg_linear_mean,
# MAGIC   ROUND(MIN(linear_mean_value), 2)  AS min_linear_mean,
# MAGIC   ROUND(MAX(linear_mean_value), 2)  AS max_linear_mean
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC WHERE is_linear_mean_suppressed = FALSE
# MAGIC GROUP BY hcahps_measure_id
# MAGIC HAVING COUNT(DISTINCT hospital_id) > 100
# MAGIC ORDER BY hospitals DESC;
# MAGIC
# MAGIC -- 2c. Star ratings (the recruiter-recognizable headline)
# MAGIC SELECT 
# MAGIC   hcahps_measure_id,
# MAGIC   MAX(hcahps_question)        AS hcahps_question,
# MAGIC   COUNT(DISTINCT hospital_id) AS hospitals,
# MAGIC   ROUND(AVG(star_rating), 2)  AS avg_stars,
# MAGIC   COUNT(CASE WHEN star_rating = 5 THEN 1 END) AS five_star_count,
# MAGIC   COUNT(CASE WHEN star_rating = 1 THEN 1 END) AS one_star_count
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC WHERE is_star_rating_suppressed = FALSE
# MAGIC GROUP BY hcahps_measure_id
# MAGIC ORDER BY hospitals DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3a. Numeric vs categorical split
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN score_numeric IS NOT NULL THEN 'numeric'
# MAGIC     WHEN score_category IS NOT NULL THEN 'categorical'
# MAGIC     ELSE 'both_null'
# MAGIC   END AS score_kind,
# MAGIC   COUNT(*)                          AS rows,
# MAGIC   COUNT(DISTINCT measure_id)        AS distinct_measures,
# MAGIC   COUNT(DISTINCT hospital_id)       AS distinct_hospitals
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC WHERE is_score_suppressed = FALSE
# MAGIC GROUP BY 1
# MAGIC ORDER BY rows DESC;
# MAGIC
# MAGIC -- 3b. Top numeric measures by coverage (these are candidates for Gold)
# MAGIC SELECT 
# MAGIC   measure_id,
# MAGIC   MAX(measure_name)                 AS measure_name,
# MAGIC   MAX(condition)                    AS condition,
# MAGIC   COUNT(DISTINCT hospital_id)       AS hospitals_reporting,
# MAGIC   COUNT(*)                          AS total_rows,
# MAGIC   ROUND(AVG(score_numeric), 2)      AS avg_score,
# MAGIC   ROUND(MIN(score_numeric), 2)      AS min_score,
# MAGIC   ROUND(MAX(score_numeric), 2)      AS max_score
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC WHERE is_score_suppressed = FALSE
# MAGIC   AND score_numeric IS NOT NULL
# MAGIC GROUP BY measure_id
# MAGIC ORDER BY hospitals_reporting DESC
# MAGIC LIMIT 30;
# MAGIC
# MAGIC -- 3c. Coverage banding (how many measures are broadly reportable vs sparse?)
# MAGIC WITH coverage AS (
# MAGIC   SELECT measure_id, COUNT(DISTINCT hospital_id) AS hospitals
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC   WHERE is_score_suppressed = FALSE AND score_numeric IS NOT NULL
# MAGIC   GROUP BY measure_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN hospitals >= 3000 THEN '1. >=3000 (broad)'
# MAGIC     WHEN hospitals >= 1000 THEN '2. 1000-2999'
# MAGIC     WHEN hospitals >= 100  THEN '3. 100-999'
# MAGIC     ELSE                        '4. <100 (sparse)'
# MAGIC   END AS coverage_band,
# MAGIC   COUNT(*) AS distinct_measures
# MAGIC FROM coverage
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4a. Hospital master — type, ownership, ER coverage breakdowns
# MAGIC SELECT 
# MAGIC   hospital_type,
# MAGIC   COUNT(*) AS hospitals,
# MAGIC   COUNT(CASE WHEN has_emergency_services THEN 1 END) AS with_er,
# MAGIC   COUNT(CASE WHEN is_overall_rating_suppressed = FALSE THEN 1 END) AS with_rating,
# MAGIC   ROUND(AVG(CASE WHEN is_overall_rating_suppressed = FALSE THEN overall_rating END), 2) AS avg_rating
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC GROUP BY hospital_type
# MAGIC ORDER BY hospitals DESC;
# MAGIC
# MAGIC -- 4b. Peer-group size check (state x hospital_type) — anything <10 can't be percentile-ranked
# MAGIC SELECT 
# MAGIC   state,
# MAGIC   hospital_type,
# MAGIC   COUNT(*) AS peer_group_size
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC GROUP BY state, hospital_type
# MAGIC HAVING COUNT(*) < 10
# MAGIC ORDER BY peer_group_size, state;
# MAGIC
# MAGIC -- 4c. Freshness across the three measure tables
# MAGIC SELECT 'readmission' AS source, 
# MAGIC        MIN(start_date) AS min_start, MAX(end_date) AS max_end, COUNT(*) AS rows
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC UNION ALL
# MAGIC SELECT 'patient_experience', 
# MAGIC        MIN(start_date), MAX(end_date), COUNT(*)
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC UNION ALL
# MAGIC SELECT 'care_measure',
# MAGIC        MIN(start_date), MAX(end_date), COUNT(*)
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3b. Top numeric measures by coverage (these are candidates for Gold)
# MAGIC SELECT 
# MAGIC   measure_id,
# MAGIC   MAX(measure_name)                 AS measure_name,
# MAGIC   MAX(condition)                    AS condition,
# MAGIC   COUNT(DISTINCT hospital_id)       AS hospitals_reporting,
# MAGIC   COUNT(*)                          AS total_rows,
# MAGIC   ROUND(AVG(score_numeric), 2)      AS avg_score,
# MAGIC   ROUND(MIN(score_numeric), 2)      AS min_score,
# MAGIC   ROUND(MAX(score_numeric), 2)      AS max_score
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC WHERE is_score_suppressed = FALSE
# MAGIC   AND score_numeric IS NOT NULL
# MAGIC GROUP BY measure_id
# MAGIC ORDER BY hospitals_reporting DESC
# MAGIC LIMIT 30;
# MAGIC
# MAGIC -- 3c. Coverage banding (how many measures are broadly reportable vs sparse?)
# MAGIC WITH coverage AS (
# MAGIC   SELECT measure_id, COUNT(DISTINCT hospital_id) AS hospitals
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC   WHERE is_score_suppressed = FALSE AND score_numeric IS NOT NULL
# MAGIC   GROUP BY measure_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN hospitals >= 3000 THEN '1. >=3000 (broad)'
# MAGIC     WHEN hospitals >= 1000 THEN '2. 1000-2999'
# MAGIC     WHEN hospitals >= 100  THEN '3. 100-999'
# MAGIC     ELSE                        '4. <100 (sparse)'
# MAGIC   END AS coverage_band,
# MAGIC   COUNT(*) AS distinct_measures
# MAGIC FROM coverage
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   measure_id,
# MAGIC   MAX(measure_name)                 AS measure_name,
# MAGIC   MAX(condition)                    AS condition,
# MAGIC   COUNT(DISTINCT hospital_id)       AS hospitals_reporting,
# MAGIC   COUNT(*)                          AS total_rows,
# MAGIC   ROUND(AVG(score_numeric), 2)      AS avg_score,
# MAGIC   ROUND(MIN(score_numeric), 2)      AS min_score,
# MAGIC   ROUND(MAX(score_numeric), 2)      AS max_score
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC WHERE is_score_suppressed = FALSE
# MAGIC   AND score_numeric IS NOT NULL
# MAGIC GROUP BY measure_id
# MAGIC ORDER BY hospitals_reporting DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_id FROM workspace.hajera_lakehouse_silver.silver_care_measure WHERE measure_id LIKE 'SAFE%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_name 
# MAGIC FROM workspace.hajera_lakehouse_silver.silver_readmission_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- gold_hospital_scorecard — development view
# MAGIC -- One paste; statement 1 creates the view, statements A–E inspect it.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- STATEMENT 1 — Create the dev view (run this first, just once)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE OR REPLACE TEMP VIEW gold_scorecard_dev AS
# MAGIC WITH
# MAGIC hospital_master AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     hospital_name,
# MAGIC     state,
# MAGIC     city,
# MAGIC     county,
# MAGIC     hospital_type,
# MAGIC     hospital_ownership,
# MAGIC     has_emergency_services,
# MAGIC     CASE WHEN is_overall_rating_suppressed THEN NULL ELSE overall_rating END
# MAGIC       AS cms_overall_rating
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC ),
# MAGIC
# MAGIC readm_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     measure_name,
# MAGIC     excess_readmission_ratio,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY measure_name
# MAGIC       ORDER BY excess_readmission_ratio DESC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC   WHERE is_excess_ratio_suppressed = FALSE
# MAGIC ),
# MAGIC readm_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HF-HRRP'       THEN excess_readmission_ratio END) AS readm_hf_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-PN-HRRP'       THEN excess_readmission_ratio END) AS readm_pn_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-AMI-HRRP'      THEN excess_readmission_ratio END) AS readm_ami_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-COPD-HRRP'     THEN excess_readmission_ratio END) AS readm_copd_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HIP-KNEE-HRRP' THEN excess_readmission_ratio END) AS readm_hipknee_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-CABG-HRRP'     THEN excess_readmission_ratio END) AS readm_cabg_excess_ratio,
# MAGIC     COUNT(*) AS readm_measures_reported,
# MAGIC     AVG(CASE
# MAGIC           WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                 'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC           THEN pct_rank
# MAGIC         END) AS readm_composite_national_pct
# MAGIC   FROM readm_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC hcahps_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     hcahps_measure_id,
# MAGIC     star_rating,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY hcahps_measure_id
# MAGIC       ORDER BY star_rating ASC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_star_rating_suppressed = FALSE
# MAGIC     AND hcahps_measure_id IN ('H_STAR_RATING',
# MAGIC                               'H_HSP_RATING_STAR_RATING',
# MAGIC                               'H_RECMND_STAR_RATING')
# MAGIC ),
# MAGIC hcahps_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_STAR_RATING'            THEN star_rating END) AS hcahps_summary_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_HSP_RATING_STAR_RATING' THEN star_rating END) AS hcahps_overall_rating_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_RECMND_STAR_RATING'     THEN star_rating END) AS hcahps_recommend_star,
# MAGIC     AVG(pct_rank) AS hcahps_composite_national_pct
# MAGIC   FROM hcahps_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC hcahps_response AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(response_rate_percent) AS hcahps_response_rate_pct
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_response_rate_suppressed = FALSE
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC care_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     measure_id,
# MAGIC     score_numeric,
# MAGIC     CASE
# MAGIC       WHEN measure_id IN ('IMM_3','SEP_1')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric ASC)
# MAGIC       WHEN measure_id IN ('OP_18b','SAFE_USE_OF_OPIOIDS')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric DESC)
# MAGIC     END AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC   WHERE is_score_suppressed = FALSE
# MAGIC     AND score_numeric IS NOT NULL
# MAGIC     AND measure_id IN ('IMM_3','OP_18b','SAFE_USE_OF_OPIOIDS','SEP_1')
# MAGIC ),
# MAGIC care_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_id = 'IMM_3'               THEN score_numeric END) AS care_imm3_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'OP_18b'              THEN score_numeric END) AS care_op18b_minutes,
# MAGIC     MAX(CASE WHEN measure_id = 'SAFE_USE_OF_OPIOIDS' THEN score_numeric END) AS care_safe_opioids_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'SEP_1'               THEN score_numeric END) AS care_sep1_pct,
# MAGIC     COUNT(*) AS care_measures_reported,
# MAGIC     AVG(pct_rank) AS care_composite_national_pct
# MAGIC   FROM care_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC stitched AS (
# MAGIC   SELECT
# MAGIC     m.hospital_id, m.hospital_name, m.state, m.city, m.county,
# MAGIC     m.hospital_type, m.hospital_ownership, m.has_emergency_services,
# MAGIC     m.cms_overall_rating,
# MAGIC
# MAGIC     r.readm_hf_excess_ratio, r.readm_pn_excess_ratio, r.readm_ami_excess_ratio,
# MAGIC     r.readm_copd_excess_ratio, r.readm_hipknee_excess_ratio, r.readm_cabg_excess_ratio,
# MAGIC     COALESCE(r.readm_measures_reported, 0) AS readm_measures_reported,
# MAGIC     r.readm_composite_national_pct,
# MAGIC
# MAGIC     h.hcahps_summary_star, h.hcahps_overall_rating_star, h.hcahps_recommend_star,
# MAGIC     hr.hcahps_response_rate_pct,
# MAGIC     h.hcahps_composite_national_pct,
# MAGIC
# MAGIC     c.care_imm3_pct, c.care_op18b_minutes, c.care_safe_opioids_pct, c.care_sep1_pct,
# MAGIC     COALESCE(c.care_measures_reported, 0) AS care_measures_reported,
# MAGIC     c.care_composite_national_pct,
# MAGIC
# MAGIC     (COALESCE(r.readm_composite_national_pct, 0)
# MAGIC      + COALESCE(h.hcahps_composite_national_pct, 0)
# MAGIC      + COALESCE(c.care_composite_national_pct, 0))
# MAGIC     / NULLIF(
# MAGIC         (CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC        + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC        + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END),
# MAGIC         0
# MAGIC       ) AS quality_composite_national_pct
# MAGIC   FROM hospital_master   m
# MAGIC   LEFT JOIN readm_pivoted   r  ON m.hospital_id = r.hospital_id
# MAGIC   LEFT JOIN hcahps_pivoted  h  ON m.hospital_id = h.hospital_id
# MAGIC   LEFT JOIN hcahps_response hr ON m.hospital_id = hr.hospital_id
# MAGIC   LEFT JOIN care_pivoted    c  ON m.hospital_id = c.hospital_id
# MAGIC ),
# MAGIC
# MAGIC peer_group_stats AS (
# MAGIC   SELECT state, hospital_type, COUNT(*) AS peer_group_size
# MAGIC   FROM hospital_master
# MAGIC   GROUP BY state, hospital_type
# MAGIC ),
# MAGIC final_with_peer AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC     pgs.peer_group_size,
# MAGIC     CASE
# MAGIC       WHEN s.quality_composite_national_pct IS NULL THEN NULL
# MAGIC       ELSE PERCENT_RANK() OVER (
# MAGIC         PARTITION BY s.state, s.hospital_type
# MAGIC         ORDER BY s.quality_composite_national_pct ASC NULLS FIRST
# MAGIC       )
# MAGIC     END AS peer_group_composite_pct,
# MAGIC     (pgs.peer_group_size < 10) AS peer_group_too_small
# MAGIC   FROM stitched s
# MAGIC   LEFT JOIN peer_group_stats pgs
# MAGIC     ON s.state = pgs.state
# MAGIC    AND s.hospital_type = pgs.hospital_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   *,
# MAGIC   '2021-07-01 to 2024-06-30' AS as_of_readmission_window,
# MAGIC   '2024-04-01 to 2025-03-31' AS as_of_hcahps_window,
# MAGIC   '2024-01-01 to 2025-03-31' AS as_of_care_window,
# MAGIC   current_timestamp()        AS gold_built_at
# MAGIC FROM final_with_peer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count FROM gold_scorecard_dev;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(quality_composite_national_pct, 1) AS bucket,
# MAGIC   COUNT(*) AS hospitals
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE quality_composite_national_pct IS NOT NULL
# MAGIC GROUP BY ROUND(quality_composite_national_pct, 1)
# MAGIC ORDER BY bucket;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (readm_composite_national_pct  IS NOT NULL) AS has_readm,
# MAGIC   (hcahps_composite_national_pct IS NOT NULL) AS has_hcahps,
# MAGIC   (care_composite_national_pct   IS NOT NULL) AS has_care,
# MAGIC   COUNT(*) AS hospitals
# MAGIC FROM gold_scorecard_dev
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY hospitals DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   state, hospital_type, peer_group_size,
# MAGIC   COUNT(*) AS hospitals_with_pct,
# MAGIC   ROUND(MIN(peer_group_composite_pct), 3) AS min_pct,
# MAGIC   ROUND(MAX(peer_group_composite_pct), 3) AS max_pct
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE peer_group_composite_pct IS NOT NULL
# MAGIC GROUP BY state, hospital_type, peer_group_size
# MAGIC ORDER BY peer_group_size DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   hospital_name, state, hospital_type,
# MAGIC   ROUND(quality_composite_national_pct, 3) AS composite,
# MAGIC   readm_measures_reported, care_measures_reported,
# MAGIC   hcahps_summary_star, cms_overall_rating
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE quality_composite_national_pct IS NOT NULL
# MAGIC ORDER BY quality_composite_national_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gold_scorecard_dev AS
# MAGIC WITH
# MAGIC hospital_master AS (
# MAGIC   SELECT
# MAGIC     hospital_id, hospital_name, state, city, county,
# MAGIC     hospital_type, hospital_ownership, has_emergency_services,
# MAGIC     CASE WHEN is_overall_rating_suppressed THEN NULL ELSE overall_rating END
# MAGIC       AS cms_overall_rating
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC ),
# MAGIC
# MAGIC readm_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, measure_name, excess_readmission_ratio,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY measure_name ORDER BY excess_readmission_ratio DESC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC   WHERE is_excess_ratio_suppressed = FALSE
# MAGIC ),
# MAGIC readm_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HF-HRRP'       THEN excess_readmission_ratio END) AS readm_hf_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-PN-HRRP'       THEN excess_readmission_ratio END) AS readm_pn_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-AMI-HRRP'      THEN excess_readmission_ratio END) AS readm_ami_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-COPD-HRRP'     THEN excess_readmission_ratio END) AS readm_copd_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HIP-KNEE-HRRP' THEN excess_readmission_ratio END) AS readm_hipknee_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-CABG-HRRP'     THEN excess_readmission_ratio END) AS readm_cabg_excess_ratio,
# MAGIC     COUNT(*) AS readm_measures_reported,
# MAGIC     SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                    'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC              THEN 1 ELSE 0 END) AS readm_broad_measures_reported,
# MAGIC     -- Composite: NULL unless >= 2 of the 4 broad measures reported
# MAGIC     CASE
# MAGIC       WHEN SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                           'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC                     THEN 1 ELSE 0 END) >= 2
# MAGIC       THEN AVG(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                           'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC                     THEN pct_rank END)
# MAGIC       ELSE NULL
# MAGIC     END AS readm_composite_national_pct
# MAGIC   FROM readm_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC hcahps_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, hcahps_measure_id, star_rating,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY hcahps_measure_id ORDER BY star_rating ASC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_star_rating_suppressed = FALSE
# MAGIC     AND hcahps_measure_id IN ('H_STAR_RATING',
# MAGIC                               'H_HSP_RATING_STAR_RATING',
# MAGIC                               'H_RECMND_STAR_RATING')
# MAGIC ),
# MAGIC hcahps_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_STAR_RATING'            THEN star_rating END) AS hcahps_summary_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_HSP_RATING_STAR_RATING' THEN star_rating END) AS hcahps_overall_rating_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_RECMND_STAR_RATING'     THEN star_rating END) AS hcahps_recommend_star,
# MAGIC     COUNT(*) AS hcahps_measures_reported,
# MAGIC     -- Composite: NULL unless all 3 stars present (HCAHPS reporting is all-or-nothing in this dataset)
# MAGIC     CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS hcahps_composite_national_pct
# MAGIC   FROM hcahps_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC hcahps_response AS (
# MAGIC   SELECT hospital_id, MAX(response_rate_percent) AS hcahps_response_rate_pct
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_response_rate_suppressed = FALSE
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC care_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, measure_id, score_numeric,
# MAGIC     CASE
# MAGIC       WHEN measure_id IN ('IMM_3','SEP_1')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric ASC)
# MAGIC       WHEN measure_id IN ('OP_18b','SAFE_USE_OF_OPIOIDS')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric DESC)
# MAGIC     END AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC   WHERE is_score_suppressed = FALSE
# MAGIC     AND score_numeric IS NOT NULL
# MAGIC     AND measure_id IN ('IMM_3','OP_18b','SAFE_USE_OF_OPIOIDS','SEP_1')
# MAGIC ),
# MAGIC care_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_id = 'IMM_3'               THEN score_numeric END) AS care_imm3_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'OP_18b'              THEN score_numeric END) AS care_op18b_minutes,
# MAGIC     MAX(CASE WHEN measure_id = 'SAFE_USE_OF_OPIOIDS' THEN score_numeric END) AS care_safe_opioids_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'SEP_1'               THEN score_numeric END) AS care_sep1_pct,
# MAGIC     COUNT(*) AS care_measures_reported,
# MAGIC     -- Composite: NULL unless >= 3 of 4 measures reported
# MAGIC     CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS care_composite_national_pct
# MAGIC   FROM care_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC stitched AS (
# MAGIC   SELECT
# MAGIC     m.hospital_id, m.hospital_name, m.state, m.city, m.county,
# MAGIC     m.hospital_type, m.hospital_ownership, m.has_emergency_services,
# MAGIC     m.cms_overall_rating,
# MAGIC
# MAGIC     r.readm_hf_excess_ratio, r.readm_pn_excess_ratio, r.readm_ami_excess_ratio,
# MAGIC     r.readm_copd_excess_ratio, r.readm_hipknee_excess_ratio, r.readm_cabg_excess_ratio,
# MAGIC     COALESCE(r.readm_measures_reported, 0) AS readm_measures_reported,
# MAGIC     COALESCE(r.readm_broad_measures_reported, 0) AS readm_broad_measures_reported,
# MAGIC     r.readm_composite_national_pct,
# MAGIC
# MAGIC     h.hcahps_summary_star, h.hcahps_overall_rating_star, h.hcahps_recommend_star,
# MAGIC     hr.hcahps_response_rate_pct,
# MAGIC     COALESCE(h.hcahps_measures_reported, 0) AS hcahps_measures_reported,
# MAGIC     h.hcahps_composite_national_pct,
# MAGIC
# MAGIC     c.care_imm3_pct, c.care_op18b_minutes, c.care_safe_opioids_pct, c.care_sep1_pct,
# MAGIC     COALESCE(c.care_measures_reported, 0) AS care_measures_reported,
# MAGIC     c.care_composite_national_pct,
# MAGIC
# MAGIC     -- Count of qualified domains contributing to top-line
# MAGIC     ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END))
# MAGIC       AS composite_domains_used,
# MAGIC
# MAGIC     -- Top-line: NULL unless >= 2 qualified domains
# MAGIC     CASE
# MAGIC       WHEN ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC            + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC            + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END)) >= 2
# MAGIC       THEN (COALESCE(r.readm_composite_national_pct, 0)
# MAGIC             + COALESCE(h.hcahps_composite_national_pct, 0)
# MAGIC             + COALESCE(c.care_composite_national_pct, 0))
# MAGIC            / NULLIF(
# MAGIC                (CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC               + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC               + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END), 0)
# MAGIC       ELSE NULL
# MAGIC     END AS quality_composite_national_pct
# MAGIC   FROM hospital_master   m
# MAGIC   LEFT JOIN readm_pivoted   r  ON m.hospital_id = r.hospital_id
# MAGIC   LEFT JOIN hcahps_pivoted  h  ON m.hospital_id = h.hospital_id
# MAGIC   LEFT JOIN hcahps_response hr ON m.hospital_id = hr.hospital_id
# MAGIC   LEFT JOIN care_pivoted    c  ON m.hospital_id = c.hospital_id
# MAGIC ),
# MAGIC
# MAGIC peer_group_stats AS (
# MAGIC   SELECT state, hospital_type, COUNT(*) AS peer_group_size
# MAGIC   FROM hospital_master
# MAGIC   GROUP BY state, hospital_type
# MAGIC ),
# MAGIC final_with_peer AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC     pgs.peer_group_size,
# MAGIC     CASE
# MAGIC       WHEN s.quality_composite_national_pct IS NULL THEN NULL
# MAGIC       ELSE PERCENT_RANK() OVER (
# MAGIC         PARTITION BY s.state, s.hospital_type
# MAGIC         ORDER BY s.quality_composite_national_pct ASC NULLS FIRST
# MAGIC       )
# MAGIC     END AS peer_group_composite_pct,
# MAGIC     (pgs.peer_group_size < 10) AS peer_group_too_small
# MAGIC   FROM stitched s
# MAGIC   LEFT JOIN peer_group_stats pgs
# MAGIC     ON s.state = pgs.state AND s.hospital_type = pgs.hospital_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   *,
# MAGIC   '2021-07-01 to 2024-06-30' AS as_of_readmission_window,
# MAGIC   '2024-04-01 to 2025-03-31' AS as_of_hcahps_window,
# MAGIC   '2024-01-01 to 2025-03-31' AS as_of_care_window,
# MAGIC   current_timestamp()        AS gold_built_at
# MAGIC FROM final_with_peer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   hospital_name, state, hospital_type,
# MAGIC   ROUND(quality_composite_national_pct, 3) AS composite,
# MAGIC   readm_measures_reported, care_measures_reported,
# MAGIC   hcahps_summary_star, cms_overall_rating
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE quality_composite_national_pct IS NOT NULL
# MAGIC ORDER BY quality_composite_national_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cms_overall_rating,
# MAGIC   COUNT(*) AS hospitals,
# MAGIC   ROUND(AVG(quality_composite_national_pct), 3) AS avg_composite,
# MAGIC   ROUND(MIN(quality_composite_national_pct), 3) AS min_composite,
# MAGIC   ROUND(MAX(quality_composite_national_pct), 3) AS max_composite
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE quality_composite_national_pct IS NOT NULL
# MAGIC GROUP BY cms_overall_rating
# MAGIC ORDER BY cms_overall_rating;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.hajera_lakehouse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- gold_scorecard_dev_view.sql
# MAGIC -- Development view for the Gold hospital scorecard (Weekend 3, Block 3.1)
# MAGIC --
# MAGIC -- Run this against workspace.hajera_lakehouse_silver.* to recreate the
# MAGIC -- session-scoped temp view used during Gold iteration. Once spot-checks
# MAGIC -- and validation queries pass, this logic gets wrapped into the
# MAGIC -- 04_gold_scorecard.py notebook with a MERGE write into Gold.
# MAGIC --
# MAGIC -- NOTE: this file is pure SQL. If you paste it into a Databricks
# MAGIC -- notebook cell whose default language is Python, prepend a line with
# MAGIC -- just "%sql" at the top of the cell. In the Databricks SQL Editor
# MAGIC -- (Workspace > SQL Editor), no magic command is needed.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gold_scorecard_dev AS
# MAGIC WITH
# MAGIC hospital_master AS (
# MAGIC   SELECT
# MAGIC     hospital_id, hospital_name, state, city, county,
# MAGIC     hospital_type, hospital_ownership, has_emergency_services,
# MAGIC     CASE WHEN is_overall_rating_suppressed THEN NULL ELSE overall_rating END
# MAGIC       AS cms_overall_rating
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_hospital
# MAGIC ),
# MAGIC
# MAGIC readm_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, measure_name, excess_readmission_ratio,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY measure_name ORDER BY excess_readmission_ratio DESC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
# MAGIC   WHERE is_excess_ratio_suppressed = FALSE
# MAGIC ),
# MAGIC readm_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HF-HRRP'       THEN excess_readmission_ratio END) AS readm_hf_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-PN-HRRP'       THEN excess_readmission_ratio END) AS readm_pn_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-AMI-HRRP'      THEN excess_readmission_ratio END) AS readm_ami_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-COPD-HRRP'     THEN excess_readmission_ratio END) AS readm_copd_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-HIP-KNEE-HRRP' THEN excess_readmission_ratio END) AS readm_hipknee_excess_ratio,
# MAGIC     MAX(CASE WHEN measure_name = 'READM-30-CABG-HRRP'     THEN excess_readmission_ratio END) AS readm_cabg_excess_ratio,
# MAGIC     COUNT(*) AS readm_measures_reported,
# MAGIC     SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                    'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC              THEN 1 ELSE 0 END) AS readm_broad_measures_reported,
# MAGIC     -- Domain composite: NULL unless >= 2 of the 4 broad measures reported.
# MAGIC     -- Matches CMS minimum-measure-count gating for star rating publication.
# MAGIC     CASE
# MAGIC       WHEN SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                           'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC                     THEN 1 ELSE 0 END) >= 2
# MAGIC       THEN AVG(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
# MAGIC                                           'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
# MAGIC                     THEN pct_rank END)
# MAGIC       ELSE NULL
# MAGIC     END AS readm_composite_national_pct
# MAGIC   FROM readm_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC hcahps_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, hcahps_measure_id, star_rating,
# MAGIC     PERCENT_RANK() OVER (
# MAGIC       PARTITION BY hcahps_measure_id ORDER BY star_rating ASC
# MAGIC     ) AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_star_rating_suppressed = FALSE
# MAGIC     AND hcahps_measure_id IN ('H_STAR_RATING',
# MAGIC                               'H_HSP_RATING_STAR_RATING',
# MAGIC                               'H_RECMND_STAR_RATING')
# MAGIC ),
# MAGIC hcahps_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_STAR_RATING'            THEN star_rating END) AS hcahps_summary_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_HSP_RATING_STAR_RATING' THEN star_rating END) AS hcahps_overall_rating_star,
# MAGIC     MAX(CASE WHEN hcahps_measure_id = 'H_RECMND_STAR_RATING'     THEN star_rating END) AS hcahps_recommend_star,
# MAGIC     COUNT(*) AS hcahps_measures_reported,
# MAGIC     -- Domain composite: NULL unless all 3 stars present
# MAGIC     -- (HCAHPS reporting is all-or-nothing in this dataset; the gate is a safety net)
# MAGIC     CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS hcahps_composite_national_pct
# MAGIC   FROM hcahps_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC hcahps_response AS (
# MAGIC   SELECT hospital_id, MAX(response_rate_percent) AS hcahps_response_rate_pct
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_patient_experience
# MAGIC   WHERE is_response_rate_suppressed = FALSE
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC care_with_pct AS (
# MAGIC   SELECT
# MAGIC     hospital_id, measure_id, score_numeric,
# MAGIC     -- Polarity per measure:
# MAGIC     --   IMM_3, SEP_1                       : higher is better (ORDER BY ASC)
# MAGIC     --   OP_18b, SAFE_USE_OF_OPIOIDS        : lower is better  (ORDER BY DESC)
# MAGIC     -- In both cases, higher pct_rank = better quality.
# MAGIC     CASE
# MAGIC       WHEN measure_id IN ('IMM_3','SEP_1')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric ASC)
# MAGIC       WHEN measure_id IN ('OP_18b','SAFE_USE_OF_OPIOIDS')
# MAGIC         THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric DESC)
# MAGIC     END AS pct_rank
# MAGIC   FROM workspace.hajera_lakehouse_silver.silver_care_measure
# MAGIC   WHERE is_score_suppressed = FALSE
# MAGIC     AND score_numeric IS NOT NULL
# MAGIC     AND measure_id IN ('IMM_3','OP_18b','SAFE_USE_OF_OPIOIDS','SEP_1')
# MAGIC ),
# MAGIC care_pivoted AS (
# MAGIC   SELECT
# MAGIC     hospital_id,
# MAGIC     MAX(CASE WHEN measure_id = 'IMM_3'               THEN score_numeric END) AS care_imm3_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'OP_18b'              THEN score_numeric END) AS care_op18b_minutes,
# MAGIC     MAX(CASE WHEN measure_id = 'SAFE_USE_OF_OPIOIDS' THEN score_numeric END) AS care_safe_opioids_pct,
# MAGIC     MAX(CASE WHEN measure_id = 'SEP_1'               THEN score_numeric END) AS care_sep1_pct,
# MAGIC     COUNT(*) AS care_measures_reported,
# MAGIC     -- Domain composite: NULL unless >= 3 of 4 measures reported
# MAGIC     CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS care_composite_national_pct
# MAGIC   FROM care_with_pct
# MAGIC   GROUP BY hospital_id
# MAGIC ),
# MAGIC
# MAGIC stitched AS (
# MAGIC   SELECT
# MAGIC     m.hospital_id, m.hospital_name, m.state, m.city, m.county,
# MAGIC     m.hospital_type, m.hospital_ownership, m.has_emergency_services,
# MAGIC     m.cms_overall_rating,
# MAGIC
# MAGIC     r.readm_hf_excess_ratio, r.readm_pn_excess_ratio, r.readm_ami_excess_ratio,
# MAGIC     r.readm_copd_excess_ratio, r.readm_hipknee_excess_ratio, r.readm_cabg_excess_ratio,
# MAGIC     COALESCE(r.readm_measures_reported, 0) AS readm_measures_reported,
# MAGIC     COALESCE(r.readm_broad_measures_reported, 0) AS readm_broad_measures_reported,
# MAGIC     r.readm_composite_national_pct,
# MAGIC
# MAGIC     h.hcahps_summary_star, h.hcahps_overall_rating_star, h.hcahps_recommend_star,
# MAGIC     hr.hcahps_response_rate_pct,
# MAGIC     COALESCE(h.hcahps_measures_reported, 0) AS hcahps_measures_reported,
# MAGIC     h.hcahps_composite_national_pct,
# MAGIC
# MAGIC     c.care_imm3_pct, c.care_op18b_minutes, c.care_safe_opioids_pct, c.care_sep1_pct,
# MAGIC     COALESCE(c.care_measures_reported, 0) AS care_measures_reported,
# MAGIC     c.care_composite_national_pct,
# MAGIC
# MAGIC     -- Count of qualified domains contributing to the top-line composite
# MAGIC     ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END))
# MAGIC       AS composite_domains_used,
# MAGIC
# MAGIC     -- Top-line: NULL unless >= 2 qualified domain composites available
# MAGIC     CASE
# MAGIC       WHEN ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC            + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC            + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END)) >= 2
# MAGIC       THEN (COALESCE(r.readm_composite_national_pct, 0)
# MAGIC             + COALESCE(h.hcahps_composite_national_pct, 0)
# MAGIC             + COALESCE(c.care_composite_national_pct, 0))
# MAGIC            / NULLIF(
# MAGIC                (CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
# MAGIC               + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
# MAGIC               + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END), 0)
# MAGIC       ELSE NULL
# MAGIC     END AS quality_composite_national_pct
# MAGIC   FROM hospital_master   m
# MAGIC   LEFT JOIN readm_pivoted   r  ON m.hospital_id = r.hospital_id
# MAGIC   LEFT JOIN hcahps_pivoted  h  ON m.hospital_id = h.hospital_id
# MAGIC   LEFT JOIN hcahps_response hr ON m.hospital_id = hr.hospital_id
# MAGIC   LEFT JOIN care_pivoted    c  ON m.hospital_id = c.hospital_id
# MAGIC ),
# MAGIC
# MAGIC peer_group_stats AS (
# MAGIC   SELECT state, hospital_type, COUNT(*) AS peer_group_size
# MAGIC   FROM hospital_master
# MAGIC   GROUP BY state, hospital_type
# MAGIC ),
# MAGIC final_with_peer AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC     pgs.peer_group_size,
# MAGIC     -- Peer-group percentile: re-rank within state x hospital_type
# MAGIC     -- NULLS FIRST puts unqualified hospitals at rank 0, then non-NULL
# MAGIC     -- composites get the remaining (1 - null_share) of the 0..1 range.
# MAGIC     CASE
# MAGIC       WHEN s.quality_composite_national_pct IS NULL THEN NULL
# MAGIC       ELSE PERCENT_RANK() OVER (
# MAGIC         PARTITION BY s.state, s.hospital_type
# MAGIC         ORDER BY s.quality_composite_national_pct ASC NULLS FIRST
# MAGIC       )
# MAGIC     END AS peer_group_composite_pct,
# MAGIC     (pgs.peer_group_size < 10) AS peer_group_too_small
# MAGIC   FROM stitched s
# MAGIC   LEFT JOIN peer_group_stats pgs
# MAGIC     ON s.state = pgs.state AND s.hospital_type = pgs.hospital_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   *,
# MAGIC   '2021-07-01 to 2024-06-30' AS as_of_readmission_window,
# MAGIC   '2024-04-01 to 2025-03-31' AS as_of_hcahps_window,
# MAGIC   '2024-01-01 to 2025-03-31' AS as_of_care_window,
# MAGIC   current_timestamp()        AS gold_built_at
# MAGIC FROM final_with_peer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cms_overall_rating,
# MAGIC   COUNT(*) AS hospitals,
# MAGIC   ROUND(AVG(quality_composite_national_pct), 3) AS avg_composite,
# MAGIC   ROUND(MIN(quality_composite_national_pct), 3) AS min_composite,
# MAGIC   ROUND(MAX(quality_composite_national_pct), 3) AS max_composite
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE quality_composite_national_pct IS NOT NULL
# MAGIC GROUP BY cms_overall_rating
# MAGIC ORDER BY cms_overall_rating;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   composite_domains_used,
# MAGIC   COUNT(*) AS hospitals,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM gold_scorecard_dev
# MAGIC GROUP BY composite_domains_used
# MAGIC ORDER BY composite_domains_used DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   hospital_name, state, hospital_type,
# MAGIC   ROUND(quality_composite_national_pct, 3) AS composite,
# MAGIC   cms_overall_rating, hcahps_summary_star,
# MAGIC   readm_broad_measures_reported, care_measures_reported
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE UPPER(hospital_name) LIKE ANY(
# MAGIC   'MAYO CLINIC%', 'CLEVELAND CLINIC%', '%JOHNS HOPKINS%',
# MAGIC   '%MASSACHUSETTS GENERAL%', '%CEDARS-SINAI%', '%UCLA%', '%UCSF%',
# MAGIC   '%NYU LANGONE%', '%MOUNT SINAI%', '%STANFORD HOSPITAL%',
# MAGIC   '%DUKE UNIVERSITY HOSPITAL%', '%BRIGHAM AND WOMEN%'
# MAGIC )
# MAGIC ORDER BY composite DESC NULLS LAST;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   hospital_name, state, hospital_type, peer_group_size,
# MAGIC   ROUND(quality_composite_national_pct, 3) AS national_pct,
# MAGIC   ROUND(peer_group_composite_pct, 3) AS peer_group_pct,
# MAGIC   cms_overall_rating
# MAGIC FROM gold_scorecard_dev
# MAGIC WHERE UPPER(hospital_name) IN (
# MAGIC   'MAYO CLINIC HOSPITAL ROCHESTER',
# MAGIC   'MAYO CLINIC HOSPITAL',
# MAGIC   'JOHNS HOPKINS HOSPITAL, THE',
# MAGIC   'DUKE UNIVERSITY HOSPITAL',
# MAGIC   'NYU LANGONE HOSPITALS',
# MAGIC   'MASSACHUSETTS GENERAL HOSPITAL',
# MAGIC   'CLEVELAND CLINIC',
# MAGIC   'CEDARS-SINAI MEDICAL CENTER'
# MAGIC )
# MAGIC ORDER BY peer_group_pct DESC NULLS LAST;
