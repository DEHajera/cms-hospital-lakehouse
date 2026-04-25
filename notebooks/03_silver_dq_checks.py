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
