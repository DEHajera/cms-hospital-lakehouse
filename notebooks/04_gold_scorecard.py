# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: hospital quality scorecard
# MAGIC
# MAGIC Denormalized, BI-ready table with one row per `(hospital_id, snapshot_year)`.
# MAGIC The scaffold for this notebook left a TODO; this file is the implementation.
# MAGIC
# MAGIC **Inputs (Silver)**
# MAGIC - `{CATALOG_NAME}.{SILVER_SCHEMA}.silver_hospital`
# MAGIC - `{CATALOG_NAME}.{SILVER_SCHEMA}.silver_readmission_measure`
# MAGIC - `{CATALOG_NAME}.{SILVER_SCHEMA}.silver_patient_experience`
# MAGIC - `{CATALOG_NAME}.{SILVER_SCHEMA}.silver_care_measure`
# MAGIC
# MAGIC **Output (Gold)**
# MAGIC - `{CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard`
# MAGIC
# MAGIC **Grain**
# MAGIC One row per `(hospital_id, snapshot_year)`. Every hospital in `silver_hospital`
# MAGIC gets a row for the current snapshot, even if every measure is NULL - preserves
# MAGIC the "data desert" signal rather than hiding non-reporting hospitals. The
# MAGIC `snapshot_year` column supports accumulating quarterly/annual history without
# MAGIC changing the schema.
# MAGIC
# MAGIC **Idempotency**
# MAGIC Safe to re-run. Uses MERGE on `(hospital_id, snapshot_year)`. Re-running in
# MAGIC the same year updates existing rows; running in a new year creates a new
# MAGIC snapshot side-by-side with prior years.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design notes
# MAGIC
# MAGIC ### Composite structure
# MAGIC The top-line composite is built from three domain composites:
# MAGIC
# MAGIC | Domain | Measures | Polarity |
# MAGIC | --- | --- | --- |
# MAGIC | Readmission | HF, PN, AMI, COPD (broad) + HIP-KNEE, CABG (specialty) | Lower excess-readmission ratio is better |
# MAGIC | Patient experience (HCAHPS) | Summary star, Overall hospital star, Recommend star | Higher star is better |
# MAGIC | Care quality | IMM_3, OP_18b, SAFE_USE_OF_OPIOIDS, SEP_1 | Per-measure (see below) |
# MAGIC
# MAGIC ### Polarity per care measure
# MAGIC - `IMM_3`  (healthcare worker flu vaccination, %)         - higher is better
# MAGIC - `SEP_1`  (severe sepsis bundle compliance, %)           - higher is better
# MAGIC - `OP_18b` (median ED time before departure, minutes)     - lower is better
# MAGIC - `SAFE_USE_OF_OPIOIDS` (concurrent prescribing, %)       - lower is better
# MAGIC
# MAGIC In all cases, higher percentile rank = better quality (polarity is flipped
# MAGIC at rank time via ORDER BY ASC vs DESC).
# MAGIC
# MAGIC ### Minimum-measure thresholds (modeled on CMS star rating gating)
# MAGIC - Readmission domain composite: requires >= 2 of (HF, PN, AMI, COPD)
# MAGIC - HCAHPS domain composite: requires all 3 stars (HCAHPS reporting is
# MAGIC   all-or-nothing in this dataset)
# MAGIC - Care domain composite: requires >= 3 of the 4 measures
# MAGIC - Top-line composite: requires >= 2 qualified domain composites
# MAGIC
# MAGIC ### Peer-group ranking
# MAGIC Each hospital is also percentile-ranked within its `(state, hospital_type)`
# MAGIC cohort. This corrects for case-mix differences across hospital types.
# MAGIC Cohorts with fewer than 10 hospitals are flagged via `peer_group_too_small`.
# MAGIC
# MAGIC ### Freshness windows
# MAGIC The three Silver sources use different CMS publication periods, declared
# MAGIC explicitly rather than hidden:
# MAGIC - Readmission (HRRP): 2021-07-01 to 2024-06-30 (3-year pooled)
# MAGIC - HCAHPS: 2024-04-01 to 2025-03-31 (most recent 12-month CMS release)
# MAGIC - Care measures: 2024-01-01 to 2025-03-31
# MAGIC
# MAGIC ### Implementation note
# MAGIC SQL statements are executed via `spark.sql(f"...")` rather than `%sql` magic
# MAGIC cells because `${VAR}` substitution from `%run`-imported Python variables
# MAGIC doesn't work reliably on serverless (the platform treats `${VAR}` as a SQL
# MAGIC parameter marker, not a string substitution). Python f-strings interpolate
# MAGIC the catalog/schema names cleanly and match the scaffold's own convention.
# MAGIC
# MAGIC ### Deviations from original scaffold
# MAGIC - `READM_30_HOSP_WIDE` is not in this dataset - only the 6 condition-specific
# MAGIC   HRRP measures are. Composite uses HF+PN+AMI+COPD excess ratios.
# MAGIC - HCAHPS uses star ratings (universal coverage, recruiter-recognizable)
# MAGIC   rather than per-measure top-box percentages.
# MAGIC - Liquid clustering on `(state, hospital_type, snapshot_year)` replaces
# MAGIC   `partitionBy("state")` + ZORDER, matching the Silver tables.
# MAGIC - MERGE replaces `overwrite` write mode (scaffold itself flagged that as
# MAGIC   a learning shortcut).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Config

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

GOLD_TABLE = "gold_hospital_scorecard"
GOLD_FQN = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{GOLD_TABLE}"

print(f"Target Gold table: {GOLD_FQN}")
print(f"Silver schema:     {CATALOG_NAME}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the staging view
# MAGIC
# MAGIC Mirrors `sql/gold_scorecard_dev_view.sql` - the iteration-tested transformation
# MAGIC validated against Silver with spot-checks A-E plus three external-validity
# MAGIC queries (CMS monotonic correlation, qualification rate, peer-group correction
# MAGIC for named academic medical centers).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW gold_scorecard_staging AS
WITH
hospital_master AS (
  SELECT
    hospital_id, hospital_name, state, city, county,
    hospital_type, hospital_ownership, has_emergency_services,
    CASE WHEN is_overall_rating_suppressed THEN NULL ELSE overall_rating END
      AS cms_overall_rating
  FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_hospital
),

readm_with_pct AS (
  SELECT
    hospital_id, measure_name, excess_readmission_ratio,
    PERCENT_RANK() OVER (
      PARTITION BY measure_name ORDER BY excess_readmission_ratio DESC
    ) AS pct_rank
  FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_readmission_measure
  WHERE is_excess_ratio_suppressed = FALSE
),
readm_pivoted AS (
  SELECT
    hospital_id,
    MAX(CASE WHEN measure_name = 'READM-30-HF-HRRP'       THEN excess_readmission_ratio END) AS readm_hf_excess_ratio,
    MAX(CASE WHEN measure_name = 'READM-30-PN-HRRP'       THEN excess_readmission_ratio END) AS readm_pn_excess_ratio,
    MAX(CASE WHEN measure_name = 'READM-30-AMI-HRRP'      THEN excess_readmission_ratio END) AS readm_ami_excess_ratio,
    MAX(CASE WHEN measure_name = 'READM-30-COPD-HRRP'     THEN excess_readmission_ratio END) AS readm_copd_excess_ratio,
    MAX(CASE WHEN measure_name = 'READM-30-HIP-KNEE-HRRP' THEN excess_readmission_ratio END) AS readm_hipknee_excess_ratio,
    MAX(CASE WHEN measure_name = 'READM-30-CABG-HRRP'     THEN excess_readmission_ratio END) AS readm_cabg_excess_ratio,
    COUNT(*) AS readm_measures_reported,
    SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
                                   'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
             THEN 1 ELSE 0 END) AS readm_broad_measures_reported,
    CASE
      WHEN SUM(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
                                          'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
                    THEN 1 ELSE 0 END) >= 2
      THEN AVG(CASE WHEN measure_name IN ('READM-30-HF-HRRP','READM-30-PN-HRRP',
                                          'READM-30-AMI-HRRP','READM-30-COPD-HRRP')
                    THEN pct_rank END)
      ELSE NULL
    END AS readm_composite_national_pct
  FROM readm_with_pct
  GROUP BY hospital_id
),

hcahps_with_pct AS (
  SELECT
    hospital_id, hcahps_measure_id, star_rating,
    PERCENT_RANK() OVER (
      PARTITION BY hcahps_measure_id ORDER BY star_rating ASC
    ) AS pct_rank
  FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_patient_experience
  WHERE is_star_rating_suppressed = FALSE
    AND hcahps_measure_id IN ('H_STAR_RATING',
                              'H_HSP_RATING_STAR_RATING',
                              'H_RECMND_STAR_RATING')
),
hcahps_pivoted AS (
  SELECT
    hospital_id,
    MAX(CASE WHEN hcahps_measure_id = 'H_STAR_RATING'            THEN star_rating END) AS hcahps_summary_star,
    MAX(CASE WHEN hcahps_measure_id = 'H_HSP_RATING_STAR_RATING' THEN star_rating END) AS hcahps_overall_rating_star,
    MAX(CASE WHEN hcahps_measure_id = 'H_RECMND_STAR_RATING'     THEN star_rating END) AS hcahps_recommend_star,
    COUNT(*) AS hcahps_measures_reported,
    CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS hcahps_composite_national_pct
  FROM hcahps_with_pct
  GROUP BY hospital_id
),
hcahps_response AS (
  SELECT hospital_id, MAX(response_rate_percent) AS hcahps_response_rate_pct
  FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_patient_experience
  WHERE is_response_rate_suppressed = FALSE
  GROUP BY hospital_id
),

care_with_pct AS (
  SELECT
    hospital_id, measure_id, score_numeric,
    CASE
      WHEN measure_id IN ('IMM_3','SEP_1')
        THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric ASC)
      WHEN measure_id IN ('OP_18b','SAFE_USE_OF_OPIOIDS')
        THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric DESC)
    END AS pct_rank
  FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_care_measure
  WHERE is_score_suppressed = FALSE
    AND score_numeric IS NOT NULL
    AND measure_id IN ('IMM_3','OP_18b','SAFE_USE_OF_OPIOIDS','SEP_1')
),
care_pivoted AS (
  SELECT
    hospital_id,
    MAX(CASE WHEN measure_id = 'IMM_3'               THEN score_numeric END) AS care_imm3_pct,
    MAX(CASE WHEN measure_id = 'OP_18b'              THEN score_numeric END) AS care_op18b_minutes,
    MAX(CASE WHEN measure_id = 'SAFE_USE_OF_OPIOIDS' THEN score_numeric END) AS care_safe_opioids_pct,
    MAX(CASE WHEN measure_id = 'SEP_1'               THEN score_numeric END) AS care_sep1_pct,
    COUNT(*) AS care_measures_reported,
    CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS care_composite_national_pct
  FROM care_with_pct
  GROUP BY hospital_id
),

stitched AS (
  SELECT
    m.hospital_id, m.hospital_name, m.state, m.city, m.county,
    m.hospital_type, m.hospital_ownership, m.has_emergency_services,
    m.cms_overall_rating,

    r.readm_hf_excess_ratio, r.readm_pn_excess_ratio, r.readm_ami_excess_ratio,
    r.readm_copd_excess_ratio, r.readm_hipknee_excess_ratio, r.readm_cabg_excess_ratio,
    COALESCE(r.readm_measures_reported, 0) AS readm_measures_reported,
    COALESCE(r.readm_broad_measures_reported, 0) AS readm_broad_measures_reported,
    r.readm_composite_national_pct,

    h.hcahps_summary_star, h.hcahps_overall_rating_star, h.hcahps_recommend_star,
    hr.hcahps_response_rate_pct,
    COALESCE(h.hcahps_measures_reported, 0) AS hcahps_measures_reported,
    h.hcahps_composite_national_pct,

    c.care_imm3_pct, c.care_op18b_minutes, c.care_safe_opioids_pct, c.care_sep1_pct,
    COALESCE(c.care_measures_reported, 0) AS care_measures_reported,
    c.care_composite_national_pct,

    ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END))
      AS composite_domains_used,

    CASE
      WHEN ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END)) >= 2
      THEN (COALESCE(r.readm_composite_national_pct, 0)
            + COALESCE(h.hcahps_composite_national_pct, 0)
            + COALESCE(c.care_composite_national_pct, 0))
           / NULLIF(
               (CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END), 0)
      ELSE NULL
    END AS quality_composite_national_pct
  FROM hospital_master   m
  LEFT JOIN readm_pivoted   r  ON m.hospital_id = r.hospital_id
  LEFT JOIN hcahps_pivoted  h  ON m.hospital_id = h.hospital_id
  LEFT JOIN hcahps_response hr ON m.hospital_id = hr.hospital_id
  LEFT JOIN care_pivoted    c  ON m.hospital_id = c.hospital_id
),

peer_group_stats AS (
  SELECT state, hospital_type, COUNT(*) AS peer_group_size
  FROM hospital_master
  GROUP BY state, hospital_type
),
final_with_peer AS (
  SELECT
    s.*,
    pgs.peer_group_size,
    CASE
      WHEN s.quality_composite_national_pct IS NULL THEN NULL
      ELSE PERCENT_RANK() OVER (
        PARTITION BY s.state, s.hospital_type
        ORDER BY s.quality_composite_national_pct ASC NULLS FIRST
      )
    END AS peer_group_composite_pct,
    (pgs.peer_group_size < 10) AS peer_group_too_small
  FROM stitched s
  LEFT JOIN peer_group_stats pgs
    ON s.state = pgs.state AND s.hospital_type = pgs.hospital_type
)

SELECT
  *,
  YEAR(current_timestamp())  AS snapshot_year,
  '2021-07-01 to 2024-06-30' AS as_of_readmission_window,
  '2024-04-01 to 2025-03-31' AS as_of_hcahps_window,
  '2024-01-01 to 2025-03-31' AS as_of_care_window,
  current_timestamp()                              AS _gold_ts,
  CAST(current_timestamp() AS STRING)              AS _source_batch_id
FROM final_with_peer
""")

print("[OK] Staging view created: gold_scorecard_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the Gold table (first run only)
# MAGIC
# MAGIC Explicit schema declared at table creation. Liquid clustering on
# MAGIC `(state, hospital_type, snapshot_year)` because those are the dashboard's
# MAGIC primary filter columns. `IF NOT EXISTS` makes this idempotent on rerun.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard (
  -- Grain
  hospital_id                       STRING  NOT NULL,
  snapshot_year                     INT     NOT NULL,

  -- Master attributes
  hospital_name                     STRING,
  state                             STRING,
  city                              STRING,
  county                            STRING,
  hospital_type                     STRING,
  hospital_ownership                STRING,
  has_emergency_services            BOOLEAN,
  cms_overall_rating                INT,

  -- Readmission domain
  readm_hf_excess_ratio             DECIMAL(10,4),
  readm_pn_excess_ratio             DECIMAL(10,4),
  readm_ami_excess_ratio            DECIMAL(10,4),
  readm_copd_excess_ratio           DECIMAL(10,4),
  readm_hipknee_excess_ratio        DECIMAL(10,4),
  readm_cabg_excess_ratio           DECIMAL(10,4),
  readm_measures_reported           INT,
  readm_broad_measures_reported     INT,
  readm_composite_national_pct      DOUBLE,

  -- Patient experience (HCAHPS) domain
  hcahps_summary_star               INT,
  hcahps_overall_rating_star        INT,
  hcahps_recommend_star             INT,
  hcahps_response_rate_pct          DECIMAL(5,2),
  hcahps_measures_reported          INT,
  hcahps_composite_national_pct     DOUBLE,

  -- Care quality domain
  care_imm3_pct                     DECIMAL(7,2),
  care_op18b_minutes                DECIMAL(8,2),
  care_safe_opioids_pct             DECIMAL(7,2),
  care_sep1_pct                     DECIMAL(7,2),
  care_measures_reported            INT,
  care_composite_national_pct       DOUBLE,

  -- Top-line composite + peer-group
  composite_domains_used            INT,
  quality_composite_national_pct    DOUBLE,
  peer_group_size                   INT,
  peer_group_composite_pct          DOUBLE,
  peer_group_too_small              BOOLEAN,

  -- Provenance + audit
  as_of_readmission_window          STRING,
  as_of_hcahps_window               STRING,
  as_of_care_window                 STRING,
  _gold_ts                          TIMESTAMP,
  _source_batch_id                  STRING
)
USING DELTA
CLUSTER BY (state, hospital_type, snapshot_year)
COMMENT 'Composite hospital quality scorecard. One row per (hospital_id, snapshot_year). National and peer-group percentile ranks across readmission, patient experience, and care quality domains. See notebook 04_gold_scorecard for design notes.'
""")

print(f"[OK] Table ready: {GOLD_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE staging -> Gold
# MAGIC
# MAGIC Idempotent upsert on `(hospital_id, snapshot_year)`. Same-year re-runs update
# MAGIC existing rows; future-year runs create new snapshot rows side-by-side.

# COMMAND ----------

merge_result = spark.sql(f"""
MERGE INTO {CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard AS target
USING gold_scorecard_staging AS source
  ON target.hospital_id = source.hospital_id
 AND target.snapshot_year = source.snapshot_year
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("[OK] MERGE complete:")
merge_result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OPTIMIZE
# MAGIC
# MAGIC Liquid clustering handles physical layout automatically; OPTIMIZE compacts
# MAGIC small files written by the MERGE. No ZORDER needed since clustering keys
# MAGIC are already declared on the table.

# COMMAND ----------

optimize_result = spark.sql(
    f"OPTIMIZE {CATALOG_NAME}.{GOLD_SCHEMA}.gold_hospital_scorecard"
)
print("[OK] OPTIMIZE complete:")
optimize_result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smoke tests
# MAGIC
# MAGIC Assertions that should hold every run. If any fail, this cell raises and
# MAGIC the notebook stops - a broken Gold rebuild can't silently propagate.

# COMMAND ----------

# Test 1: Current-year row count matches silver_hospital exactly
silver_hospital_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {CATALOG_NAME}.{SILVER_SCHEMA}.silver_hospital"
).collect()[0]["n"]

current_year_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {GOLD_FQN} WHERE snapshot_year = YEAR(current_timestamp())"
).collect()[0]["n"]

assert current_year_count == silver_hospital_count, (
    f"Row count mismatch: Gold current-year has {current_year_count}, "
    f"Silver hospital has {silver_hospital_count}"
)
print(f"[OK] Current-year row count: Gold={current_year_count}, Silver={silver_hospital_count}")

# COMMAND ----------

# Test 2: composite_domains_used in valid range [0, 3]
bad_domains = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {GOLD_FQN}
    WHERE composite_domains_used < 0 OR composite_domains_used > 3
""").collect()[0]["n"]

assert bad_domains == 0, f"Found {bad_domains} rows with composite_domains_used outside [0,3]"
print(f"[OK] composite_domains_used all in [0,3]")

# COMMAND ----------

# Test 3: composite percentiles in valid range [0, 1] or NULL
bad_pcts = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {GOLD_FQN}
    WHERE (quality_composite_national_pct IS NOT NULL
           AND (quality_composite_national_pct < 0 OR quality_composite_national_pct > 1))
       OR (peer_group_composite_pct IS NOT NULL
           AND (peer_group_composite_pct < 0 OR peer_group_composite_pct > 1))
""").collect()[0]["n"]

assert bad_pcts == 0, f"Found {bad_pcts} rows with percentile outside [0,1]"
print(f"[OK] All composite percentiles in [0,1]")

# COMMAND ----------

# Test 4: Cross-validation - composite monotonically increases with CMS rating
cms_correlation = spark.sql(f"""
    SELECT
      cms_overall_rating,
      ROUND(AVG(quality_composite_national_pct), 3) AS avg_composite
    FROM {GOLD_FQN}
    WHERE quality_composite_national_pct IS NOT NULL
      AND cms_overall_rating IS NOT NULL
      AND snapshot_year = YEAR(current_timestamp())
    GROUP BY cms_overall_rating
    ORDER BY cms_overall_rating
""").collect()

ratings = [(row["cms_overall_rating"], row["avg_composite"]) for row in cms_correlation]
prev_avg = None
for rating, avg in ratings:
    if prev_avg is not None:
        assert avg > prev_avg, (
            f"CMS rating {rating} has avg_composite {avg} <= previous rating avg {prev_avg}. "
            f"Composite should monotonically increase with CMS rating."
        )
    prev_avg = avg

print(f"[OK] Composite monotonically increases with CMS overall rating:")
for rating, avg in ratings:
    print(f"     CMS {rating}-star -> avg composite {avg}")

# COMMAND ----------

# Test 5: At least 50% of hospitals are rankable (have a national composite)
rankable_pct = spark.sql(f"""
    SELECT 100.0 * SUM(CASE WHEN quality_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct
    FROM {GOLD_FQN}
    WHERE snapshot_year = YEAR(current_timestamp())
""").collect()[0]["pct"]

assert rankable_pct >= 50.0, f"Only {rankable_pct:.1f}% of hospitals are rankable; expected >= 50%"
print(f"[OK] {rankable_pct:.1f}% of hospitals have a national composite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC Gold scorecard rebuilt successfully. Next:
# MAGIC - `05_gold_benchmarks` builds the peer-benchmarking tables that depend on this
# MAGIC - `sql/dashboard_queries.sql` provides the 10 reference queries that pull from
# MAGIC   this table for the Databricks SQL dashboard

# COMMAND ----------

# Final summary print
summary = spark.sql(f"""
    SELECT
      COUNT(DISTINCT snapshot_year) AS snapshot_years,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN snapshot_year = YEAR(current_timestamp()) THEN 1 ELSE 0 END) AS current_year_rows,
      SUM(CASE WHEN quality_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END) AS rankable_rows,
      ROUND(MIN(quality_composite_national_pct), 3) AS min_composite,
      ROUND(MAX(quality_composite_national_pct), 3) AS max_composite,
      ROUND(AVG(quality_composite_national_pct), 3) AS avg_composite,
      MAX(_gold_ts) AS last_built_at
    FROM {GOLD_FQN}
""").collect()[0]

print("=" * 60)
print(f"Gold table:          {GOLD_FQN}")
print(f"Snapshot years:      {summary['snapshot_years']}")
print(f"Total rows:          {summary['total_rows']:,}")
print(f"Current year rows:   {summary['current_year_rows']:,}")
print(f"Rankable rows:       {summary['rankable_rows']:,}")
print(f"Composite range:     [{summary['min_composite']}, {summary['max_composite']}]")
print(f"Composite mean:      {summary['avg_composite']}")
print(f"Built at:            {summary['last_built_at']}")
print("=" * 60)
