# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Workspace setup
# MAGIC
# MAGIC Creates the catalogs, schemas, and volume used by the rest of the pipeline.
# MAGIC **Run this notebook first** after cloning the repo into Databricks Repos.
# MAGIC
# MAGIC This notebook is idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Change `SCHEMA_PREFIX` and `OWNER_EMAIL` before first run.

# COMMAND ----------

# === CHANGE THESE TWO VALUES ===
SCHEMA_PREFIX = "hajera_lakehouse"              # your-name_lakehouse
OWNER_EMAIL   = "DEHajera@gmail.com"        # your email
# ===============================

CATALOG_NAME = "workspace"  # Community Edition default
VOLUME_NAME  = "cms_raw"

# Derived names
BRONZE_SCHEMA = f"{SCHEMA_PREFIX}_bronze"
SILVER_SCHEMA = f"{SCHEMA_PREFIX}_silver"
GOLD_SCHEMA   = f"{SCHEMA_PREFIX}_gold"
VOLUME_PATH   = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

print(f"Catalog : {CATALOG_NAME}")
print(f"Schemas : {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
print(f"Volume  : {VOLUME_PATH}")
print(f"Owner   : {OWNER_EMAIL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schemas
# MAGIC
# MAGIC Separate schemas for each medallion layer is intentional — makes permissions, quotas, and retention easy to reason about (this is a DBA instinct that carries over directly).

# COMMAND ----------

for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema} COMMENT 'CMS hospital lakehouse project — owner: {OWNER_EMAIL}'")
    print(f"✓ {CATALOG_NAME}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the raw-data volume
# MAGIC
# MAGIC Unity Catalog volumes are the modern equivalent of DBFS mount points — a governed filesystem location. On Community Edition (no UC), we fall back to a DBFS path.

# COMMAND ----------

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.{VOLUME_NAME}")
    print(f"✓ Volume: {VOLUME_PATH}")
    RAW_PATH = VOLUME_PATH
except Exception as e:
    # Community Edition fallback — no UC volumes
    RAW_PATH = f"/dbfs/tmp/{SCHEMA_PREFIX}/{VOLUME_NAME}"
    dbutils.fs.mkdirs(RAW_PATH.replace("/dbfs", "dbfs:"))
    print(f"! Volumes not available (expected on Community Edition). Using DBFS: {RAW_PATH}")
    print(f"  Reason: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke test
# MAGIC
# MAGIC Confirm we can read and write from the path.

# COMMAND ----------

test_df = spark.createDataFrame([(1, "ok")], ["id", "status"])
test_path = f"{RAW_PATH}/_smoke_test"
test_df.write.mode("overwrite").format("delta").save(test_path.replace("/dbfs", "dbfs:"))
print(f"✓ Wrote smoke-test Delta to {test_path}")

reread = spark.read.format("delta").load(test_path.replace("/dbfs", "dbfs:"))
assert reread.count() == 1, "Smoke test failed"
print(f"✓ Read back {reread.count()} row — setup OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export config for downstream notebooks
# MAGIC
# MAGIC Downstream notebooks import this via `%run ./00_setup` at the top.

# COMMAND ----------

# Make variables available to notebooks that %run this one
print("Config variables defined: CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, VOLUME_PATH, RAW_PATH")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.hajera_lakehouse_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'hcahps' AS source, _batch_id, COUNT(*) AS rows
# MAGIC FROM workspace.hajera_lakehouse_bronze.bronze_hcahps GROUP BY _batch_id
# MAGIC UNION ALL
# MAGIC SELECT 'hospital_general', _batch_id, COUNT(*)
# MAGIC FROM workspace.hajera_lakehouse_bronze.bronze_hospital_general GROUP BY _batch_id
# MAGIC UNION ALL
# MAGIC SELECT 'readmissions', _batch_id, COUNT(*)
# MAGIC FROM workspace.hajera_lakehouse_bronze.bronze_readmissions GROUP BY _batch_id
# MAGIC UNION ALL
# MAGIC SELECT 'timely_care', _batch_id, COUNT(*)
# MAGIC FROM workspace.hajera_lakehouse_bronze.bronze_timely_care GROUP BY _batch_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_version();
# MAGIC
