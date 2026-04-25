# Build Plan — ~25 Hours Across 3 Weekends

This is the hour-by-hour plan to go from an empty repo to a fully-working, recruiter-ready lakehouse project. Designed to fit into three Saturdays of ~8 hours each, or spread across weekday evenings.

**Total:** ~25 hours of focused work, plus ~3 hours of polish (README screenshots, LinkedIn post, walkthrough recording).

---

## Weekend 1 — Foundation (8 hours)

### Block 1.1 — Environment setup (2 hours)
- [ ] Create the GitHub repo (`cms-hospital-lakehouse`).
- [ ] Clone it locally; copy this scaffold into it; initial commit & push.
- [ ] Sign up for Databricks Community Edition.
- [ ] Connect Databricks Repos to your GitHub repo (see `SETUP.md` §3).
- [ ] Create a single-node cluster (Runtime 14.3 LTS ML).
- [ ] Open `notebooks/00_setup.py`; customize `SCHEMA_PREFIX` and `OWNER_EMAIL`.

**Deliverable at the end of this block:** `00_setup.py` runs cleanly; you can see your `hajera_lakehouse_bronze`, `_silver`, `_gold` schemas in the Data tab.

### Block 1.2 — PySpark warm-up (1 hour)
- [ ] If Python/PySpark is rusty: run through the first 3 notebooks of Databricks' ["Get started"](https://docs.databricks.com/en/getting-started/quick-start.html) tutorial. ~45 minutes.
- [ ] Test reading a public CSV into a Spark DataFrame in a scratch notebook.

### Block 1.3 — Bronze ingestion (3 hours)
- [ ] Open `notebooks/01_bronze_ingest.py`. Read the notebook-level markdown explaining what Bronze should do.
- [ ] Implement the four `download_and_land()` calls for the four CMS datasets (URLs provided in the notebook).
- [ ] Verify each Bronze Delta table has `_ingest_ts`, `_source_file`, `_batch_id` audit columns.
- [ ] Run `DESCRIBE HISTORY bronze.hospital_general` and observe the transaction log.
- [ ] Re-run the whole notebook. Confirm the `_batch_id` logic correctly skips already-ingested batches (idempotency).

**Deliverable:** Four Bronze tables, each with row counts that match the source CSVs. Bronze is complete.

### Block 1.4 — Commit & journal (2 hours)
- [ ] Commit and push your Bronze work.
- [ ] Write a 200-word entry in a scratch `notes/week1.md` file: what worked, what surprised you, what you'd do differently. (This becomes the seed of your LinkedIn post in week 3.)
- [ ] Skim `ARCHITECTURE.md` again with fresh eyes — did you deviate? Note it.

---

## Weekend 2 — Silver + DQ (8 hours)

### Block 2.1 — Silver transformations (3 hours)
- [ ] Open `notebooks/02_silver_clean.py`.
- [ ] For each Silver table: cast types explicitly (especially `hospital_id` as `STRING`), trim/upper-case standardization, dedupe by `ROW_NUMBER()`.
- [ ] Use `MERGE INTO` for all Silver writes (the scaffold shows the pattern).
- [ ] Partition Silver tables by `state`.

**Pro tip from a DBA:** write your `MERGE` statements as if source and target might have colliding rows. Think of it as an idempotent `MERGE` in T-SQL — same mental model, different syntax.

### Block 2.2 — Data quality checks (3 hours)
- [ ] Open `notebooks/03_silver_dq_checks.py`.
- [ ] Implement the 6 check types documented in `ARCHITECTURE.md`:
  - schema contract (non-null + unique keys)
  - null-rate thresholds
  - range checks
  - referential integrity
  - freshness
  - volume
- [ ] Every check writes a summary row to `silver.dq_run_summary`.
- [ ] Hard-fail checks raise an exception; soft-fail checks log and continue.

**Deliverable:** A populated `dq_run_summary` table. Run `SELECT * FROM silver.dq_run_summary ORDER BY run_ts DESC LIMIT 20` — screenshot for the README.

### Block 2.3 — `OPTIMIZE` + small-file cleanup (1 hour)
- [ ] End of each Silver notebook, run `OPTIMIZE silver.<table> ZORDER BY (_silver_ts)`.
- [ ] Run `VACUUM` with retention of 7 days.
- [ ] Note the size difference in the Delta file listing before/after `OPTIMIZE`. This is a DBA win — show it in your README.

### Block 2.4 — Commit & journal (1 hour)
- [ ] Commit and push.
- [ ] Week 2 journal entry. Capture the moment where your DBA brain noticed a problem your Spark brain didn't (there will be one).

---

## Weekend 3 — Gold, dashboard, polish (9 hours)

### Block 3.1 — Gold scorecard (2 hours)
- [ ] Open `notebooks/04_gold_scorecard.py`.
- [ ] Join Silver tables into one row per (hospital_id, year). Include: overall rating, readmission rate, HCAHPS communication score, timely-care score, peer percentiles.
- [ ] Partition by `state`, Z-order by `(year, hospital_id)`.

### Block 3.2 — Peer benchmarks (1.5 hours)
- [ ] Open `notebooks/05_gold_benchmarks.py`.
- [ ] Compute percentile rank within (state, bed-count band, hospital type).
- [ ] Write to `gold.peer_benchmarks`.

### Block 3.3 — Dashboard (2 hours)
- [ ] Open `sql/dashboard_queries.sql` and paste each query into a notebook `%sql` cell (or into Databricks SQL if on Free Trial).
- [ ] Build 4 visuals: state-level readmission map, top-10 scorecard, bottom-10 scorecard, peer-percentile histogram.
- [ ] Screenshot the dashboard. Save as `images/dashboard.png`.

### Block 3.4 — Fill in the README outcomes (1 hour)
- [ ] Update the `[X]` placeholders in the README with your real numbers:
  - Row counts ingested (Bronze)
  - Number of DQ expectations (Silver)
  - Gold scorecard row count
  - End-to-end pipeline runtime (run it all once, wall-clock)
  - Data volume (MB ingested)
- [ ] Add architecture diagram + dashboard screenshot to `images/`.

### Block 3.5 — Record a walkthrough (1 hour)
- [ ] Using Loom, OBS, or macOS QuickTime: record a 90-second screen capture.
- [ ] Walk through: the architecture diagram in the README, then clicking run on each notebook, then the dashboard.
- [ ] Upload to YouTube as unlisted, or to Loom. Link it in the README under "Walkthrough."

### Block 3.6 — LinkedIn post + repo polish (1.5 hours)
- [ ] Customize your LinkedIn URL to `linkedin.com/in/hajerakhan` (or similar).
- [ ] Publish a LinkedIn post: "I spent 3 weekends building my first Databricks lakehouse. Here's what a decade as a DBA taught me that helped — and what it didn't." Link the repo. (Template in `docs/linkedin_post_template.md`.)
- [ ] Polish the repo: double-check README renders correctly on GitHub, all links work, images load, LICENSE is present.
- [ ] Pin the repo on your GitHub profile.

---

## Stretch goals (not required for Project #1 to count)

Spend another weekend on any of these if the fire is still burning — each one makes the project more impressive, but the project is *done* without them:

- [ ] Convert the pipeline into a Databricks Workflow with scheduled refresh.
- [ ] Add `pytest` unit tests for a few of the Silver cleaning functions.
- [ ] Swap `03_silver_dq_checks.py` for Great Expectations.
- [ ] Build a simple readmission-risk classifier on Silver using `mlflow`.

## Definition of done (ship criteria)

Project #1 is "done" when all of these are true:

- [ ] Repo is public on GitHub and pinned on your profile.
- [ ] README has: story, architecture diagram, outcomes with real numbers, dashboard screenshot, walkthrough link, author section with customized LinkedIn URL.
- [ ] All 6 notebooks run end-to-end from a fresh workspace with zero manual intervention.
- [ ] `ARCHITECTURE.md`, `SETUP.md`, `BUILD_PLAN.md`, `docs/data_dictionary.md` are all populated.
- [ ] LinkedIn post is live linking the repo.
- [ ] You can, from memory, give a 2-minute verbal walkthrough of the design.

When all those are checked — you don't have "a portfolio project." You have **shipped work**. That's the thing recruiters hire.
