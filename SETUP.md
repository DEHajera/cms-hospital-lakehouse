# Setup Guide

One-time setup to get the project runnable end-to-end on Databricks Community Edition. Budget ~1 hour for this the first time.

## 1. Create a GitHub repository

1. Go to [github.com/new](https://github.com/new).
2. Repository name: `cms-hospital-lakehouse`.
3. **Public**, add a MIT License (or keep the one in this project), skip the `.gitignore` (we already have one).
4. Create repo.
5. On your laptop, clone the empty repo, copy all the files from this project into it, commit, and push:
   ```bash
   git clone https://github.com/<your-username>/cms-hospital-lakehouse.git
   cd cms-hospital-lakehouse
   # copy everything from this project folder into here
   git add .
   git commit -m "Initial scaffold: README, architecture, build plan, notebook skeletons"
   git push
   ```

## 2. Sign up for Databricks Community Edition (free)

1. Go to [databricks.com/try-databricks](https://www.databricks.com/try-databricks) and choose **"Community Edition"** (the free tier, not the 14-day trial).
2. Verify your email.
3. Log in. You'll land on the Databricks workspace.

Community Edition limits you'll bump into (and how to work around them):

| Limit | Impact | Workaround |
|---|---|---|
| Single-node clusters only (6GB) | You can't run big data | The CMS dataset is <200MB total — fine |
| Clusters auto-terminate after 2 hours idle | Mid-session disconnects | Re-attach; Delta state is persistent |
| No Unity Catalog, uses `hive_metastore` | Some 3-part names differ | Notebooks use `hive_metastore.hajera_lakehouse.<table>` |
| No Databricks Workflows (jobs) | Can't schedule | Run manually; document the orchestration in BUILD_PLAN.md |
| No Databricks SQL warehouse | Dashboard uses notebook `%sql` cells | Fine for demo purposes |

> **Upgrade path:** when you're ready to show this to recruiters with a live dashboard link, spin up a **Databricks Free Trial** (14 days, full platform including SQL warehouses and Unity Catalog). Record a screen capture of the dashboard before the trial ends and link it in the README.

## 3. Connect your Databricks workspace to GitHub (Databricks Repos)

1. In Databricks, click **Workspace → Repos → Add Repo**.
2. Git repository URL: your repo URL (e.g., `https://github.com/<your-username>/cms-hospital-lakehouse.git`).
3. If prompted for auth, create a [GitHub Personal Access Token](https://github.com/settings/tokens?type=beta) with **Repo → Contents → Read and write** scope. Paste as the password.
4. Clone. You should now see `cms-hospital-lakehouse/notebooks/` inside your Databricks workspace.

## 4. Create the compute cluster

1. In Databricks, **Compute → Create compute**.
2. Cluster name: `lakehouse-single`.
3. Databricks Runtime version: **14.3 LTS ML** (or the latest LTS ML available on Community Edition).
4. Policy: **Single node**.
5. Instance type: leave default (8GB single-node on Community Edition).
6. Create and wait ~5 minutes for it to start.

## 5. Download the CMS data once

CMS publishes the data on [data.cms.gov/provider-data](https://data.cms.gov/provider-data/). For reproducibility, the `01_bronze_ingest.py` notebook downloads the CSVs directly over HTTPS into a Databricks volume. No manual download needed — the notebook handles it.

If you'd rather preview the data first:
- [Hospital General Information](https://data.cms.gov/provider-data/dataset/xubh-q36u)
- [Hospital-Level Readmissions](https://data.cms.gov/provider-data/dataset/9n3s-kdb3)
- [HCAHPS Patient Experience](https://data.cms.gov/provider-data/dataset/dgck-syfz)
- [Timely and Effective Care](https://data.cms.gov/provider-data/dataset/yv7e-xc69)

Each is 5–50 MB.

## 6. Configure the notebook (one variable to change)

Open `notebooks/00_setup.py` and change:

```python
CATALOG_NAME = "hive_metastore"         # Community Edition default; leave as-is
SCHEMA_PREFIX = "hajera_lakehouse"      # change to <your-name>_lakehouse
VOLUME_NAME   = "cms_raw"               # leave as-is unless you want a custom name
OWNER_EMAIL   = "hajerakhan@hotmail.com"  # change to your email
```

## 7. Run order

1. Attach any notebook to your cluster (top-right dropdown).
2. Run in order:
   - `00_setup.py` (creates schemas + volume; <1 minute)
   - `01_bronze_ingest.py` (downloads + lands Bronze; ~3 minutes)
   - `02_silver_clean.py` (types + dedupes + MERGEs Silver; ~2 minutes)
   - `03_silver_dq_checks.py` (DQ expectations + audit log; ~1 minute)
   - `04_gold_scorecard.py` (Gold scorecard; ~1 minute)
   - `05_gold_benchmarks.py` (peer benchmarks; ~1 minute)
3. In the last notebook, a `%sql SELECT * FROM gold.hospital_scorecard LIMIT 20` cell confirms you're live.

## 8. (Optional but recommended) Dashboard

1. In Databricks, **SQL → Dashboards → Create** (Free Trial) *or* use a notebook with `%sql` cells on Community Edition.
2. Paste queries from `sql/dashboard_queries.sql`.
3. Pin the charts: state-level readmission rate, peer-percentile distribution, top-10 and bottom-10 scorecard.
4. Screenshot the dashboard, save as `images/dashboard.png`, and reference it in the README outcomes section.

## 9. Polish for GitHub

Once the pipeline runs end-to-end:

1. Fill in the `[X]` values in the README outcomes section with your real numbers (row counts, runtime, table counts).
2. Add architecture diagram and dashboard screenshots to `images/`.
3. Update the "About the author" section of the README with your actual customized LinkedIn URL.
4. Record a 60-second screen capture of the pipeline running; post as a LinkedIn update and link in the README.

## Troubleshooting

- **`PermissionError` on CSV download:** Check your cluster has internet access (Community Edition does; some corporate trials don't).
- **`Table not found` errors:** Run `00_setup.py` first. Every notebook depends on the schemas it creates.
- **Slow runs on Community Edition:** Normal. Single-node 8GB cluster is fine for this dataset (~150MB total); if a cell takes >10 minutes, cancel and check for an accidental `collect()`.
- **Git conflicts in Databricks Repos:** Pull before you start editing, commit small, push often.
