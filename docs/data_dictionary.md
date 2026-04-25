# Data Dictionary

Field-level documentation for every Silver and Gold table.

Each section lists: column name, type, source, description, and any known quirks.

---

## Silver

### `silver_hospital`

Master dimension — one row per hospital facility.

| Column | Type | Source | Description |
|---|---|---|---|
| `hospital_id` | STRING | CMS `Facility ID` | 6-char CMS facility identifier. Preserve leading zeros — **never** cast to INT. |
| `hospital_name` | STRING | CMS `Facility Name` | Trimmed. |
| `state` | STRING | CMS `State` | Upper-cased 2-letter code. Partition column. |
| `city` | STRING | | |
| `address` | STRING | | |
| `zip_code` | STRING | | Left as string — leading zeros in NE zips. |
| `hospital_type` | STRING | | e.g., "Acute Care Hospitals", "Critical Access Hospitals". |
| `hospital_ownership` | STRING | | e.g., "Voluntary non-profit - Private". |
| `overall_rating` | INT | | 1–5. Null if CMS reports "Not Available" (~25% of hospitals). |
| `_ingest_ts` | TIMESTAMP | pipeline | When this row was ingested. |
| `_batch_id` | STRING | pipeline | Bronze ingestion batch ID. |
| `_silver_ts` | TIMESTAMP | pipeline | When this row was promoted to Silver. |

### `silver_readmission_measure`

| Column | Type | Description |
|---|---|---|
| `hospital_id` | STRING | FK → `silver_hospital.hospital_id`. |
| `measure_id` | STRING | e.g., `READM_30_HOSP_WIDE`, `READM_30_AMI` (heart attack), `READM_30_HF` (heart failure). |
| `measure_name` | STRING | Human-readable measure name. |
| `start_date`, `end_date` | DATE | Reporting period. |
| `excess_readmission_ratio` | DECIMAL(5,4) | <1.0 = fewer readmissions than predicted; >1.0 = more. |
| `predicted_readmission_rate` | DECIMAL(5,4) | Risk-adjusted rate. |
| `expected_readmission_rate` | DECIMAL(5,4) | Peer-adjusted rate. |
| `number_of_discharges` | INT | Denominator for the rate. |
| `state` | STRING | Joined from `silver_hospital` for partitioning. |
| `_ingest_ts`, `_batch_id`, `_silver_ts` | | standard audit cols |

### `silver_patient_experience`

HCAHPS patient-experience metrics. Top-box = % of respondents giving the highest rating.

| Column | Type | Description |
|---|---|---|
| `hospital_id` | STRING | FK → `silver_hospital.hospital_id`. |
| `hcahps_measure_id` | STRING | e.g., `H_COMP_1_A_P` (nurse communication top-box), `H_CLEAN_HSP_A_P` (cleanliness top-box). |
| `hcahps_question` | STRING | Human-readable question text. |
| `top_box_percent` | DECIMAL(5,2) | 0.0–100.0. |
| `bottom_box_percent` | DECIMAL(5,2) | 0.0–100.0. |
| `response_rate_percent` | DECIMAL(5,2) | Survey response rate. |
| `state` | STRING | Joined from `silver_hospital`. |
| `_ingest_ts`, `_batch_id`, `_silver_ts` | | |

### `silver_care_measure`

Timely and Effective Care metrics.

| Column | Type | Description |
|---|---|---|
| `hospital_id` | STRING | FK. |
| `measure_id` | STRING | e.g., `OP_18b` (median ED-depart time), `SEP_1` (sepsis care bundle). |
| `measure_name` | STRING | |
| `score` | STRING | CMS stores mixed numeric + "Not Available" — keep as STRING here and parse in Gold. |
| `sample` | INT | |
| `footnote` | STRING | |
| `state` | STRING | |
| `_ingest_ts`, `_batch_id`, `_silver_ts` | | |

### `dq_run_summary`

One row per (batch × table × check).

| Column | Type | Description |
|---|---|---|
| `batch_id` | STRING | |
| `run_ts` | TIMESTAMP | |
| `table` | STRING | |
| `check` | STRING | |
| `severity` | STRING | `HARD` \| `SOFT` \| `WARN` |
| `result` | STRING | `pass` \| `fail` \| `warn` |
| `rows_failed` | INT | |
| `detail` | STRING | |

---

## Gold

### `gold_hospital_scorecard`

One row per (`hospital_id`, `year`). Denormalized for BI.

| Column | Type | Description |
|---|---|---|
| `hospital_id` | STRING | |
| `hospital_name`, `state`, `hospital_type` | STRING | |
| `year` | INT | Reporting year (derived from Silver batch window). |
| `overall_rating` | INT | 1–5. |
| `readmission_rate_overall` | DECIMAL(6,4) | Hospital-wide 30-day readmission rate. |
| `excess_readmission_ratio_heart_attack` | DECIMAL(6,4) | For AMI measure. |
| `excess_readmission_ratio_heart_failure` | DECIMAL(6,4) | For HF measure. |
| `hcahps_communication_topbox` | DECIMAL(5,2) | % topbox on nurse communication. |
| `hcahps_cleanliness_topbox` | DECIMAL(5,2) | |
| `hcahps_overall_topbox` | DECIMAL(5,2) | Overall hospital rating topbox. |
| `ed_wait_time_minutes` | INT | Median time from ED arrival to departure. |
| `sepsis_care_pct` | DECIMAL(5,2) | % of sepsis patients receiving the bundle. |
| `_gold_ts` | TIMESTAMP | |
| `_source_batch_id` | STRING | |

**Partition:** `state`. **Z-order:** `(year, hospital_id)`.

### `gold_peer_benchmarks`

Same grain as scorecard, with percentile rankings within peer group.

| Column | Type | Description |
|---|---|---|
| (all scorecard columns) | | |
| `overall_rating_pctile` | DOUBLE | 0–100 percentile within (state × hospital_type). |
| `readmission_rate_pctile` | DOUBLE | Lower rate = higher percentile. |
| `patient_experience_pctile` | DOUBLE | |
| `peer_group_size` | INT | Number of hospitals in this hospital's peer group. |
