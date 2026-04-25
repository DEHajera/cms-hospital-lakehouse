-- ===========================================================================
-- Databricks SQL Dashboard Queries
-- Source: hajera_lakehouse_gold.gold_hospital_scorecard
--         hajera_lakehouse_gold.gold_peer_benchmarks
-- ===========================================================================
-- Paste each query into a separate Databricks SQL query, give it a name,
-- and build a visual. The visual type suggested in the comment is the
-- one I'd recommend for a clean dashboard.
-- ===========================================================================


-- 1. KPI tile: number of hospitals covered
-- Visual: Counter
SELECT COUNT(DISTINCT hospital_id) AS hospitals_tracked
FROM hajera_lakehouse_gold.gold_hospital_scorecard;


-- 2. KPI tile: national average overall rating
-- Visual: Counter (1 decimal place)
SELECT ROUND(AVG(overall_rating), 2) AS avg_overall_rating
FROM hajera_lakehouse_gold.gold_hospital_scorecard
WHERE overall_rating IS NOT NULL;


-- 3. Choropleth: state-level average overall rating
-- Visual: Map (filled map by state)
SELECT
  state,
  COUNT(*)                       AS n_hospitals,
  ROUND(AVG(overall_rating), 2)  AS avg_rating,
  ROUND(AVG(readmission_rate_overall) * 100, 2) AS avg_readmission_pct
FROM hajera_lakehouse_gold.gold_hospital_scorecard
WHERE overall_rating IS NOT NULL
GROUP BY state
ORDER BY n_hospitals DESC;


-- 4. Top-10 scorecard: best peer-adjusted hospitals
-- Visual: Table
SELECT
  hospital_name,
  state,
  hospital_type,
  overall_rating,
  ROUND(overall_rating_pctile, 1)  AS peer_pctile,
  peer_group_size
FROM hajera_lakehouse_gold.gold_peer_benchmarks
WHERE peer_group_size >= 10
ORDER BY overall_rating_pctile DESC
LIMIT 10;


-- 5. Bottom-10 scorecard: worst peer-adjusted hospitals
-- Visual: Table
SELECT
  hospital_name,
  state,
  hospital_type,
  overall_rating,
  ROUND(overall_rating_pctile, 1)  AS peer_pctile,
  peer_group_size
FROM hajera_lakehouse_gold.gold_peer_benchmarks
WHERE peer_group_size >= 10
ORDER BY overall_rating_pctile ASC
LIMIT 10;


-- 6. Peer-percentile distribution
-- Visual: Histogram on overall_rating_pctile (20 buckets)
SELECT
  FLOOR(overall_rating_pctile / 5) * 5 AS pctile_band,
  COUNT(*) AS n_hospitals
FROM hajera_lakehouse_gold.gold_peer_benchmarks
GROUP BY pctile_band
ORDER BY pctile_band;


-- 7. Ownership-type breakdown of readmission rates
-- Visual: Bar chart
SELECT
  hospital_type,
  ROUND(AVG(readmission_rate_overall) * 100, 2) AS avg_readmission_pct,
  COUNT(*)                                      AS n
FROM hajera_lakehouse_gold.gold_hospital_scorecard
WHERE readmission_rate_overall IS NOT NULL
GROUP BY hospital_type
ORDER BY avg_readmission_pct DESC;


-- 8. Filterable hospital lookup
-- Visual: Table (add a state filter/parameter in Databricks SQL)
-- Parameter: {{ state }}
SELECT
  h.hospital_name,
  h.state,
  h.hospital_type,
  h.overall_rating,
  ROUND(h.readmission_rate_overall * 100, 2) AS readmission_pct,
  h.hcahps_overall_topbox                    AS patient_exp_topbox_pct,
  ROUND(b.overall_rating_pctile, 1)          AS peer_pctile
FROM hajera_lakehouse_gold.gold_hospital_scorecard h
LEFT JOIN hajera_lakehouse_gold.gold_peer_benchmarks b
  ON h.hospital_id = b.hospital_id
WHERE h.state = '{{ state }}'
ORDER BY h.overall_rating DESC;


-- 9. DQ observability: most recent DQ run results
-- Visual: Table
SELECT
  run_ts,
  batch_id,
  table,
  check,
  severity,
  result,
  rows_failed,
  detail
FROM hajera_lakehouse_silver.dq_run_summary
WHERE batch_id = (SELECT MAX(batch_id) FROM hajera_lakehouse_silver.dq_run_summary)
ORDER BY severity DESC, result ASC;


-- 10. Pipeline freshness
-- Visual: Counter + conditional formatting (red if > 30 days)
SELECT
  DATEDIFF(CURRENT_TIMESTAMP(), MAX(_gold_ts)) AS days_since_last_gold_build
FROM hajera_lakehouse_gold.gold_hospital_scorecard;
