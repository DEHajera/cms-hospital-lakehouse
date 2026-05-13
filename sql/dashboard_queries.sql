-- =====================================================================
-- dashboard_queries.sql
-- Reference queries for the CMS Hospital Quality Lakehouse dashboard
--
-- Source table: workspace.hajera_lakehouse_gold.gold_hospital_scorecard
-- Built by:     notebooks/04_gold_scorecard.py
--
-- This file is the contract between the Gold layer and the Databricks SQL
-- dashboard. Each query below corresponds to one dashboard tile and is
-- annotated with:
--   - Recommended visualization type
--   - "What this proves" caption (recruiter narrative)
--   - Any filter widgets the dashboard should attach
--
-- All queries filter to snapshot_year = YEAR(current_date()) so the
-- dashboard always shows the current snapshot. When history accumulates
-- in future years, add a snapshot_year dropdown filter to the dashboard
-- and replace the hardcoded filter with :snapshot_year.
--
-- Audience: Recruiter / hiring manager browsing the portfolio.
-- =====================================================================


-- ====================================================================
-- SECTION 1: NATIONAL LEADERBOARD
-- The headline story - "we built a composite, validated it against CMS,
-- here are the top and bottom hospitals."
-- ====================================================================


-- --------------------------------------------------------------------
-- Query 1.1 - KPI: Total hospitals, rankable hospitals, freshness
-- Viz:        Counter tiles (3 single-number widgets in a row)
-- Proves:     The lakehouse covers the full national hospital population
--             and applies CMS-style minimum-measure gating to ranking
-- --------------------------------------------------------------------
SELECT
  COUNT(*)                                                                                         AS total_hospitals,
  SUM(CASE WHEN quality_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END)                      AS rankable_hospitals,
  ROUND(100.0 * SUM(CASE WHEN quality_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                                                             AS pct_rankable,
  MAX(_gold_ts)                                                                                    AS last_refreshed_at,
  MAX(as_of_readmission_window)                                                                    AS readmission_window,
  MAX(as_of_hcahps_window)                                                                         AS hcahps_window,
  MAX(as_of_care_window)                                                                           AS care_window
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date());


-- --------------------------------------------------------------------
-- Query 1.2 - National top 25 hospitals by composite
-- Viz:        Table, sorted by composite descending, with CMS rating
--             shown alongside for cross-validation
-- Proves:     Top hospitals cluster around CMS 4-5 star ratings -
--             our composite agrees with CMS's independent scoring
-- --------------------------------------------------------------------
SELECT
  hospital_name,
  state,
  hospital_type,
  ROUND(quality_composite_national_pct, 3)        AS national_composite,
  ROUND(peer_group_composite_pct, 3)              AS peer_group_composite,
  cms_overall_rating,
  hcahps_summary_star,
  composite_domains_used
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
  AND quality_composite_national_pct IS NOT NULL
ORDER BY quality_composite_national_pct DESC
LIMIT 25;


-- --------------------------------------------------------------------
-- Query 1.3 - Composite distribution histogram
-- Viz:        Bar chart, bucket on X, hospitals on Y
-- Proves:     The bell-shaped distribution centered near 0.5 is the
--             mathematical signature of averaging polarity-aware
--             percentile ranks across multiple measures - it's a
--             validity check, not a bug
-- --------------------------------------------------------------------
SELECT
  ROUND(quality_composite_national_pct, 1) AS composite_bucket,
  COUNT(*)                                 AS hospitals
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
  AND quality_composite_national_pct IS NOT NULL
GROUP BY ROUND(quality_composite_national_pct, 1)
ORDER BY composite_bucket;


-- --------------------------------------------------------------------
-- Query 1.4 - Composite vs CMS Overall Star Rating (external validity)
-- Viz:        Bar chart, CMS rating on X, avg composite on Y
-- Proves:     Each CMS star is worth ~0.066 of our composite. Strictly
--             monotonic 1->2->3->4->5. Our composite, built from
-- different measures with different methodology, correlates with CMS's
--             own published scoring. THIS IS THE KEY VALIDITY CHART.
-- --------------------------------------------------------------------
SELECT
  cms_overall_rating                                                AS cms_star_rating,
  COUNT(*)                                                          AS hospitals,
  ROUND(AVG(quality_composite_national_pct), 3)                     AS avg_composite,
  ROUND(MIN(quality_composite_national_pct), 3)                     AS min_composite,
  ROUND(MAX(quality_composite_national_pct), 3)                     AS max_composite,
  ROUND(STDDEV(quality_composite_national_pct), 3)                  AS stddev_composite
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
  AND quality_composite_national_pct IS NOT NULL
  AND cms_overall_rating IS NOT NULL
GROUP BY cms_overall_rating
ORDER BY cms_overall_rating;


-- ====================================================================
-- SECTION 2: PEER-GROUP CORRECTION
-- The methodological story - "a flat national ranking is misleading
-- because hospital types serve different populations. Here's how
-- peer-group ranking corrects for that."
-- ====================================================================


-- --------------------------------------------------------------------
-- Query 2.1 - National vs peer-group rank for named AMCs
-- Viz:        Table OR slope chart (national pct -> peer pct)
-- Proves:     Major academic medical centers (Mayo, Johns Hopkins,
--             Duke, NYU, Mass General) cluster at the 60-65th national
--             percentile but 92-99th peer-group percentile. The
--             peer-group column is load-bearing for honest interpretation.
-- --------------------------------------------------------------------
SELECT
  hospital_name,
  state,
  hospital_type,
  peer_group_size,
  ROUND(quality_composite_national_pct, 3)                                AS national_pct,
  ROUND(peer_group_composite_pct, 3)                                      AS peer_group_pct,
  ROUND(peer_group_composite_pct - quality_composite_national_pct, 3)     AS peer_correction,
  cms_overall_rating
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
  AND UPPER(hospital_name) IN (
    'MAYO CLINIC HOSPITAL ROCHESTER',
    'MAYO CLINIC HOSPITAL',
    'JOHNS HOPKINS HOSPITAL, THE',
    'DUKE UNIVERSITY HOSPITAL',
    'NYU LANGONE HOSPITALS',
    'MASSACHUSETTS GENERAL HOSPITAL',
    'CLEVELAND CLINIC',
    'CEDARS-SINAI MEDICAL CENTER'
  )
ORDER BY peer_group_pct DESC NULLS LAST;


-- --------------------------------------------------------------------
-- Query 2.2 - Composite distribution by hospital type
-- Viz:        Box plot (or grouped bar chart of avg composite per type)
-- Proves:     Critical Access Hospitals and specialty hospitals sit
--             higher than general Acute Care on raw composite -
--             reproduces the well-documented bias in CMS-style scoring
-- --------------------------------------------------------------------
SELECT
  hospital_type,
  COUNT(*)                                              AS hospitals,
  SUM(CASE WHEN quality_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END) AS rankable,
  ROUND(AVG(quality_composite_national_pct), 3)         AS avg_national,
  ROUND(AVG(peer_group_composite_pct), 3)               AS avg_peer_group,
  ROUND(STDDEV(quality_composite_national_pct), 3)      AS stddev_national
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
GROUP BY hospital_type
ORDER BY hospitals DESC;


-- --------------------------------------------------------------------
-- Query 2.3 - Peer-group reliability: how many cohorts are too small
-- Viz:        Counter tile + small bar of cohort sizes
-- Proves:     The dashboard is honest about where peer-group ranking
--             is statistically reliable (>=10 hospitals per cohort)
--             and where it isn't
-- --------------------------------------------------------------------
WITH cohorts AS (
  SELECT state, hospital_type, MAX(peer_group_size) AS peer_group_size
  FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
  WHERE snapshot_year = YEAR(current_date())
  GROUP BY state, hospital_type
)
SELECT
  CASE
    WHEN peer_group_size >= 50 THEN '1. Robust (>=50)'
    WHEN peer_group_size >= 20 THEN '2. Strong (20-49)'
    WHEN peer_group_size >= 10 THEN '3. Adequate (10-19)'
    ELSE                            '4. Too small (<10)'
  END                              AS cohort_strength,
  COUNT(*)                         AS cohort_count,
  SUM(peer_group_size)             AS hospitals_in_band
FROM cohorts
GROUP BY 1
ORDER BY 1;


-- ====================================================================
-- SECTION 3: DOMAIN DEEP-DIVES
-- The data fluency story - "we can drill into any of the three quality
-- domains independently. Here are the headline distributions for each."
-- ====================================================================


-- --------------------------------------------------------------------
-- Query 3.1 - Readmission excess ratios by condition
-- Viz:        Histogram with vertical line at 1.0 (the "expected" mark);
--             can be faceted by measure or stacked
-- Proves:     The HRRP excess ratios cluster tightly around 1.0 -
--             that's the CMS risk-adjustment working as designed
-- --------------------------------------------------------------------
SELECT 'Heart Failure'     AS condition,
       readm_hf_excess_ratio       AS excess_ratio
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND readm_hf_excess_ratio IS NOT NULL
UNION ALL
SELECT 'Pneumonia',        readm_pn_excess_ratio
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND readm_pn_excess_ratio IS NOT NULL
UNION ALL
SELECT 'Acute MI',         readm_ami_excess_ratio
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND readm_ami_excess_ratio IS NOT NULL
UNION ALL
SELECT 'COPD',             readm_copd_excess_ratio
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND readm_copd_excess_ratio IS NOT NULL;


-- --------------------------------------------------------------------
-- Query 3.2 - HCAHPS star distribution (national)
-- Viz:        Grouped bar chart, star value on X, hospitals on Y,
--             grouped/faceted by survey question
-- Proves:     Three HCAHPS measures cover the same patient population
--             with subtly different signals - communication-about-medicines
--             scores lower than overall hospital rating, recommend rate higher
-- --------------------------------------------------------------------
SELECT
  'Summary star'                AS measure,
  hcahps_summary_star           AS star_rating,
  COUNT(*)                      AS hospitals
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND hcahps_summary_star IS NOT NULL
GROUP BY hcahps_summary_star
UNION ALL
SELECT
  'Overall hospital rating',
  hcahps_overall_rating_star,
  COUNT(*)
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND hcahps_overall_rating_star IS NOT NULL
GROUP BY hcahps_overall_rating_star
UNION ALL
SELECT
  'Recommend hospital',
  hcahps_recommend_star,
  COUNT(*)
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date()) AND hcahps_recommend_star IS NOT NULL
GROUP BY hcahps_recommend_star
ORDER BY measure, star_rating;


-- --------------------------------------------------------------------
-- Query 3.3 - Care quality: ED wait time by state
-- Viz:        Choropleth map of US states (state on map, color by
--             median OP_18b minutes), or horizontal bar chart top 20 states
-- Proves:     A geographic story emerges - ED throughput varies
--             meaningfully by state, useful for any state-by-state
--             healthcare policy discussion
-- --------------------------------------------------------------------
SELECT
  state,
  COUNT(*)                                            AS hospitals_reporting,
  ROUND(AVG(care_op18b_minutes), 1)                   AS avg_ed_wait_minutes,
  ROUND(PERCENTILE(care_op18b_minutes, 0.5), 1)       AS median_ed_wait_minutes,
  ROUND(MIN(care_op18b_minutes), 1)                   AS min_ed_wait,
  ROUND(MAX(care_op18b_minutes), 1)                   AS max_ed_wait
FROM workspace.hajera_lakehouse_gold.gold_hospital_scorecard
WHERE snapshot_year = YEAR(current_date())
  AND care_op18b_minutes IS NOT NULL
GROUP BY state
HAVING COUNT(*) >= 10
ORDER BY median_ed_wait_minutes DESC;
