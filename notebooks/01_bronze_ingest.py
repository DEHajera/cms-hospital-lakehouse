# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC
# MAGIC Downloads the four CMS Care Compare CSV datasets and lands them as Delta Bronze tables.
# MAGIC
# MAGIC **Design:**
# MAGIC - Append-only writes tagged with `_batch_id` (idempotency guard).
# MAGIC - Preserves source schema exactly; no business logic in Bronze.
# MAGIC - Three audit columns added to every row: `_ingest_ts`, `_source_file`, `_batch_id`.
# MAGIC
# MAGIC **Inputs:** four CSVs hosted on data.cms.gov (~150MB total).
# MAGIC **Outputs:** four Bronze Delta tables.
# MAGIC **Runtime:** ~3 minutes on Community Edition.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import urllib.request, os, uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source definitions
# MAGIC
# MAGIC The CSV URLs below are stable CMS-provided download endpoints. If a URL breaks, find the dataset on [data.cms.gov/provider-data](https://data.cms.gov/provider-data/) and grab the current CSV link.

# COMMAND ----------

BATCH_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]

SOURCES = [
    # (logical_name, url, bronze_table)
    # Using the data.cms.gov datastore-query API endpoints rather than the
    # rotating "resources/<hash>_<name>.csv" links. The dataset IDs are stable;
    # the resource hashes rotate quarterly when CMS republishes Care Compare.
    ("hospital_general",
     "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv",
     "bronze_hospital_general"),
    ("readmissions",
     "https://data.cms.gov/provider-data/api/1/datastore/query/9n3s-kdb3/0/download?format=csv",
     "bronze_readmissions"),
    ("hcahps",
     "https://data.cms.gov/provider-data/api/1/datastore/query/dgck-syfz/0/download?format=csv",
     "bronze_hcahps"),
    ("timely_care",
     "https://data.cms.gov/provider-data/api/1/datastore/query/yv7e-xc69/0/download?format=csv",
     "bronze_timely_care"),
]

# Stability note: the four IDs (xubh-q36u, 9n3s-kdb3, dgck-syfz, yv7e-xc69)
# are CMS Provider Data Catalog dataset slugs and have been stable since 2018+.
# The /datastore/query/<id>/0/download?format=csv pattern is the documented
# CMS Provider Data API and survives quarterly Care Compare refreshes — so
# unlike the legacy resource-hash CSV links, these URLs do not need to be
# re-fetched before each run.

print(f"Batch ID: {BATCH_ID}")
for src in SOURCES:
    print(f"  {src[0]} -> {src[2]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: download a CSV into the raw volume
# MAGIC
# MAGIC Single-responsibility function — easy to test, easy to replace with an `Auto Loader` step later.

# COMMAND ----------

def download_csv(url: str, local_name: str) -> str:
    """Download a CSV to the raw volume and return the local path."""
    # RAW_PATH from 00_setup.py
    dest = f"{RAW_PATH}/{local_name}.csv"
    dest_dbfs = dest.replace("/dbfs", "dbfs:") if dest.startswith("/dbfs") else dest
    # Use urllib through the driver; writes to the DBFS-backed path
    driver_dest = f"/tmp/{local_name}.csv"
    print(f"Downloading {url} -> {driver_dest}")
    urllib.request.urlretrieve(url, driver_dest)
    # Copy into the Databricks filesystem
    dbutils.fs.cp(f"file:{driver_dest}", dest_dbfs)
    print(f"  landed at {dest}")
    return dest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: land a CSV as a Bronze Delta table
# MAGIC
# MAGIC The `_batch_id`, `_ingest_ts`, `_source_file` audit columns are the backbone of every reliability pattern in this project — add them in exactly one place.

# COMMAND ----------

def land_to_bronze(csv_path: str, table_name: str, batch_id: str) -> int:
    """Read CSV, add audit columns, write as Bronze Delta (append). Returns row count."""
    full_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .option("multiLine", True)
          .option("escape", '"')
          .csv(csv_path.replace("/dbfs", "dbfs:")))

    df_audited = (df
                  .withColumn("_ingest_ts", F.current_timestamp())
                  .withColumn("_source_file", F.lit(csv_path))
                  .withColumn("_batch_id", F.lit(batch_id)))

    # Check if this batch has already been ingested (idempotency)
    if spark.catalog.tableExists(full_table):
        already = (spark.read.table(full_table)
                   .where(F.col("_batch_id") == batch_id)
                   .count())
        if already > 0:
            print(f"  {table_name}: batch {batch_id} already ingested ({already} rows) — skipping.")
            return 0

    (df_audited.write
        .mode("append")
        .format("delta")
        .saveAsTable(full_table))

    row_count = df_audited.count()
    print(f"  {table_name}: {row_count:,} rows appended (batch {batch_id}).")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the ingestion

# COMMAND ----------

totals = {}
for logical_name, url, table_name in SOURCES:
    print(f"\n--- {logical_name} ---")
    try:
        local = download_csv(url, logical_name)
        rows = land_to_bronze(local, table_name, BATCH_ID)
        totals[table_name] = rows
    except Exception as e:
        print(f"  ERROR processing {logical_name}: {e}")
        totals[table_name] = -1

print("\n=== Summary ===")
for tbl, n in totals.items():
    status = "✓" if n >= 0 else "✗"
    print(f"  {status} {tbl}: {n:,} rows" if n >= 0 else f"  {status} {tbl}: FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Spot-check one table — structure, count, audit columns, transaction history.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace <prefix> with your SCHEMA_PREFIX if running manually
# MAGIC SELECT COUNT(*) AS n_rows, MIN(_ingest_ts) AS first_seen, MAX(_ingest_ts) AS last_seen, COUNT(DISTINCT _batch_id) AS n_batches
# MAGIC FROM hajera_lakehouse_bronze.bronze_hospital_general

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transaction log — this is your DBA-level audi