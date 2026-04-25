# CMS Hospital Quality Lakehouse

> A production-style medallion lakehouse on Databricks that ingests public CMS Hospital Care Compare data, cleans and validates it, and produces analytics-ready gold tables for hospital quality scorecards and peer benchmarking.

![Status](https://img.shields.io/badge/status-in%20progress-blue)
![Platform](https://img.shields.io/badge/platform-Databricks-red)
![Stack](https://img.shields.io/badge/stack-PySpark%20%7C%20Delta%20Lake%20%7C%20Databricks%20SQL-0B2C55)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Why I built this

I spent a decade as a SQL Server DBA and Site Reliability Engineer keeping regulated, mission-critical data systems alive — including eight years in healthcare software at PointClickCare running HIPAA-compliant environments, and my current role leading SRE for the enterprise database-as-a-service platform at Citigroup.

This project is how I'm translating that operational data-infrastructure background into modern data engineering. Same discipline — HA, data quality, reliability, documentation — applied to a modern lakehouse stack instead of an OLTP estate.

The dataset (CMS Care Compare) is the same kind of data I saw at PointClickCare, except public: hospital quality metrics, readmission rates, patient experience scores, and timely-care measures published by the Centers for Medicare & Medicaid Services. The kind of data a hospital executive or healthcare payer would want to see aggregated, benchmarked, and refreshed reliably.

## What this project demonstrates

| Capability | Evidenced by |
|---|---|
| Medallion (Bronze / Silver / Gold) lakehouse design | Three-tier pipeline under `notebooks/` |
| Delta Lake write patterns | Schema enforcement, `MERGE` upserts, time travel, `OPTIMIZE` / `VACUUM` |
| PySpark data engineering | `notebooks/02_silver_clean.py`, `notebooks/04_gold_scorecard.py` |
| Data quality engineering | Dedicated DQ notebook with expectations, null-rate checks, referential integrity |
| Modeling for analytics | Gold scorecard + peer-benchmark tables designed for BI workloads |
| Reliability mindset | Idempotent notebooks, audit columns, partitioning, data-lineage notes |
| Databricks SQL + dashboarding | Dashboard queries under `sql/` |
| Professional documentation | This README, `ARCHITECTURE.md`, `BUILD_PLAN.md`, `SETUP.md`, `docs/data_dictionary.md` |

## Architecture at a glance

```
                ┌──────────────────────┐
                │  data.cms.gov CSVs   │
                │  (public, no auth)   │
                └──────────┬───────────┘
                           │ download
                           ▼
┌──────────────────────────────────────────────────────────┐
│                     BRONZE  (raw)                        │
│  As-ingested Delta tables; schema preserved, no edits.   │
│  Adds: _ingest_ts, _source_file, _batch_id               │
└──────────────────────┬───────────────────────────────────┘
                       │  typed casts, dedupe, cleanup
                       ▼
┌──────────────────────────────────────────────────────────┐
│                    SILVER  (curated)                     │
│  Strongly-typed, deduped, standardized, joined where     │
│  it makes sense. Enforced by schema + data-quality checks│
└──────────────────────┬───────────────────────────────────┘
                       │ business aggregations
                       ▼
┌──────────────────────────────────────────────────────────┐
│                      GOLD  (serving)                     │
│  • hospital_scorecard   (one row per hospital × year)    │
│  • peer_benchmarks      (percentile rank by region/size) │
│  • readmission_trends   (time series per hospital)       │
└──────────────────────┬───────────────────────────────────┘
                       │
                       ▼
        Databricks SQL Dashboard  +  (future) RAG chatbot
```

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the design rationale — why medallion, why Delta, partition/cluster choices, data-quality strategy, and the reliability patterns borrowed from my SRE background.

## Tech stack

- **Compute & runtime:** Databricks (Community Edition is sufficient), Databricks Runtime 14.3 LTS ML or later
- **Storage format:** Delta Lake
- **Languages:** PySpark (Python 3.10+), Databricks SQL
- **Orchestration (for later):** Databricks Workflows
- **Source control:** Git / GitHub with Databricks Repos
- **Data source:** [CMS Care Compare datasets](https://data.cms.gov/provider-data/) (Hospital General Information, Hospital-Level Readmissions Reduction Program, HCAHPS Patient Experience, Timely and Effective Care)

## Repository layout

```
cms-hospital-lakehouse/
├── README.md                    ← you are here
├── ARCHITECTURE.md              ← design rationale, partitioning, DQ strategy
├── SETUP.md                     ← one-time Databricks + Git setup
├── BUILD_PLAN.md                ← hour-by-hour ~25-hour build plan
├── LICENSE                      ← MIT
├── .gitignore
├── notebooks/
│   ├── 00_setup.py              ← catalog, schema, volume, config
│   ├── 01_bronze_ingest.py      ← download CMS CSVs, land as Delta Bronze
│   ├── 02_silver_clean.py       ← cast, dedupe, standardize, MERGE into Silver
│   ├── 03_silver_dq_checks.py   ← null-rate, referential integrity, range checks
│   ├── 04_gold_scorecard.py     ← hospital quality scorecard (Gold)
│   └── 05_gold_benchmarks.py    ← peer-benchmark percentiles (Gold)
├── sql/
│   └── dashboard_queries.sql    ← queries powering the Databricks SQL dashboard
├── docs/
│   └── data_dictionary.md       ← field-level schema documentation
└── images/
    └── (add architecture diagram + dashboard screenshots here)
```

## Outcomes (fill these in as you build — these are the headline results for the README)

> The items below are written in a results-forward voice so recruiters reading the top of your README see outcomes, not activities. **Fill in the `[X]` values with your real numbers as you build.**

- Ingested **`[X]`** hospital records across **`[Y]`** CMS source tables into a Bronze Delta layer; idempotent re-runs verified via `_batch_id` audit column.
- Silver layer applies **`[N]`** data-quality expectations (null thresholds, range checks, referential integrity) with a full audit table for failed rows.
- Gold layer produces a unified `hospital_scorecard` table that would take **`[Z]` SQL Server joins** to produce in the source system, now materialized in **`[seconds]`** on Databricks.
- End-to-end pipeline runs in **`[runtime]`** on Databricks Community Edition single-node; partitioned and Z-ordered for sub-second BI queries.
- Dashboard built in Databricks SQL visualizing state-level readmission trends and peer-percentile benchmarking for **`[#]`** hospitals.

## How to reproduce

1. **One-time setup:** follow [`SETUP.md`](./SETUP.md) (Databricks Community Edition, Repos, data download).
2. **Build order:** run notebooks `00` → `05` in order. Each notebook is idempotent; you can re-run freely.
3. **Dashboard:** open `sql/dashboard_queries.sql` in Databricks SQL and build visuals from the provided queries.
4. **Full walkthrough (~25 hours across 3 weekends):** see [`BUILD_PLAN.md`](./BUILD_PLAN.md).

## Future work (roadmap)

The lakehouse is the foundation. The plan is to layer these on top, one at a time:

- [ ] **Orchestration.** Convert the notebooks into a Databricks Workflow with scheduled refresh and failure alerts.
- [ ] **Streaming ingestion.** Replace batch ingestion with Auto Loader + Delta Live Tables for the subset of datasets that update daily.
- [ ] **Feature store.** Register the scorecard features in Databricks Feature Store.
- [ ] **ML: readmission-risk model.** Train a simple gradient boosted model predicting readmission risk from the silver layer; track with MLflow.
- [ ] **RAG chatbot over quality data.** Embed hospital profiles with Databricks Vector Search; let a user ask "Which Virginia hospitals have the best heart-attack outcomes?" in natural language.
- [ ] **Observability.** Ship pipeline metrics to a Grafana dashboard (leveraging my SRE background).
- [ ] **Terraform.** Manage the workspace, cluster, and permissions as code.

## About the author

**Hajera Khan** — Senior Data Platform Engineer & SRE. 10+ years running mission-critical SQL Server data systems (HIPAA & financial-services regulated). Currently leading SRE for an enterprise SQL Server DBaaS platform at Citigroup; actively transitioning into modern data engineering and AI data pipelines.

- LinkedIn: [hajerakhan](https://www.linkedin.com/in/hajerakhan) *(update link once you customize your URL)*
- Based in Reston, VA · US Citizen · Open to hybrid / remote

## License

MIT — see [`LICENSE`](./LICENSE). Data used is public-domain CMS data; please review CMS terms of use on [data.cms.gov](https://data.cms.gov/).

---

*Built as part of a deliberate pivot from SQL Server DBA / SRE into modern data engineering and AI data pipelines. Feedback welcome — open an issue or reach out on LinkedIn.*
