-- =====================================================================
-- gold_scorecard_dev_view.sql
-- Development view for the Gold hospital scorecard (Weekend 3, Block 3.1)
--
-- Run this against workspace.hajera_lakehouse_silver.* to recreate the
-- session-scoped temp view used during Gold iteration. Once spot-checks
-- and validation queries pass, this logic gets wrapped into the
-- 04_gold_scorecard.py notebook with a MERGE write into Gold.
--
-- NOTE: this file is pure SQL. If you paste it into a Databricks
-- notebook cell whose default language is Python, prepend a line with
-- just "%sql" at the top of the cell. In the Databricks SQL Editor
-- (Workspace > SQL Editor), no magic command is needed.
-- =====================================================================

CREATE OR REPLACE TEMP VIEW gold_scorecard_dev AS
WITH
hospital_master AS (
  SELECT
    hospital_id, hospital_name, state, city, county,
    hospital_type, hospital_ownership, has_emergency_services,
    CASE WHEN is_overall_rating_suppressed THEN NULL ELSE overall_rating END
      AS cms_overall_rating
  FROM workspace.hajera_lakehouse_silver.silver_hospital
),

readm_with_pct AS (
  SELECT
    hospital_id, measure_name, excess_readmission_ratio,
    PERCENT_RANK() OVER (
      PARTITION BY measure_name ORDER BY excess_readmission_ratio DESC
    ) AS pct_rank
  FROM workspace.hajera_lakehouse_silver.silver_readmission_measure
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
    -- Domain composite: NULL unless >= 2 of the 4 broad measures reported.
    -- Matches CMS minimum-measure-count gating for star rating publication.
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
  FROM workspace.hajera_lakehouse_silver.silver_patient_experience
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
    -- Domain composite: NULL unless all 3 stars present
    -- (HCAHPS reporting is all-or-nothing in this dataset; the gate is a safety net)
    CASE WHEN COUNT(*) >= 3 THEN AVG(pct_rank) ELSE NULL END AS hcahps_composite_national_pct
  FROM hcahps_with_pct
  GROUP BY hospital_id
),
hcahps_response AS (
  SELECT hospital_id, MAX(response_rate_percent) AS hcahps_response_rate_pct
  FROM workspace.hajera_lakehouse_silver.silver_patient_experience
  WHERE is_response_rate_suppressed = FALSE
  GROUP BY hospital_id
),

care_with_pct AS (
  SELECT
    hospital_id, measure_id, score_numeric,
    -- Polarity per measure:
    --   IMM_3, SEP_1                       : higher is better (ORDER BY ASC)
    --   OP_18b, SAFE_USE_OF_OPIOIDS        : lower is better  (ORDER BY DESC)
    -- In both cases, higher pct_rank = better quality.
    CASE
      WHEN measure_id IN ('IMM_3','SEP_1')
        THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric ASC)
      WHEN measure_id IN ('OP_18b','SAFE_USE_OF_OPIOIDS')
        THEN PERCENT_RANK() OVER (PARTITION BY measure_id ORDER BY score_numeric DESC)
    END AS pct_rank
  FROM workspace.hajera_lakehouse_silver.silver_care_measure
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
    -- Domain composite: NULL unless >= 3 of 4 measures reported
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

    -- Count of qualified domains contributing to the top-line composite
    ((CASE WHEN r.readm_composite_national_pct  IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN h.hcahps_composite_national_pct IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN c.care_composite_national_pct   IS NOT NULL THEN 1 ELSE 0 END))
      AS composite_domains_used,

    -- Top-line: NULL unless >= 2 qualified domain composites available
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
    -- Peer-group percentile: re-rank within state x hospital_type
    -- NULLS FIRST puts unqualified hospitals at rank 0, then non-NULL
    -- composites get the remaining (1 - null_share) of the 0..1 range.
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
  '2021-07-01 to 2024-06-30' AS as_of_readmission_window,
  '2024-04-01 to 2025-03-31' AS as_of_hcahps_window,
  '2024-01-01 to 2025-03-31' AS as_of_care_window,
  current_timestamp()        AS gold_built_at
FROM final_with_peer;
