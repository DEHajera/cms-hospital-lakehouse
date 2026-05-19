# Week 3 raw notes - Weekend 3 wrap (DQ done, Gold shipped, dashboard published)

## May 11 session - Block 3.0 (Silver DQ harness completed)

- Block 2.4 work technically ended Weekend 2 with all 26 checks passing on the
  data and the dq_run_summary table persisting cleanly. Resumed Block 3.0 on
  May 11 to do the cleanup: confirmed all four Silver tables queryable after
  OPTIMIZE, re-ran the harness end-to-end, walked through the dq_run_summary
  history to make sure run lineage was readable. Everything held.

- One open thread from Week 2: the SOFT warn on overall_rating null rate (47%
  vs 25% threshold). Decided to leave the threshold strict and the warn
  visible. The right move - either I tune the threshold to match observed CMS
  suppression and lose the alerting signal, or I leave it strict and the WARN
  acts as a living record that "yes, this is real, and yes, I checked." Future
  me will thank current me for not silently hiding it.

## May 11 session - Block 3.1 starts (Gold discovery)

- Did three rounds of DESCRIBE queries against Silver to ground the Gold design
  in real column names rather than guessing. Caught two scaffold assumptions
  immediately:

  - The scaffold's column list assumed READM_30_HOSP_WIDE existed in the CMS
    data. It doesn't - this dataset only ships the 6 condition-specific HRRP
    measures (HF, PN, AMI, COPD, HIP-KNEE, CABG). Composite had to be rebuilt
    around HF+PN+AMI+COPD with the two specialty measures as drill-downs.

  - The scaffold assumed HCAHPS top-box percentages. Discovery showed all 9
    star ratings have universal coverage (3,179 hospitals, all-or-nothing
    reporting). Stars are more recruiter-recognizable and analytically cleaner
    than top-box percentages, so the design switched to stars.

- Lesson: scaffolds reflect what someone imagined the data would look like.
  Real data tells you what it actually looks like. Discovery before design,
  every time.

- Built the Gold scorecard as a TEMP VIEW first (gold_scorecard_dev) to
  iterate against. Walked through 5 spot-checks (row count, composite
  distribution, domain reporting matrix, peer-group sanity, leaderboard) and
  3 external-validity queries (CMS monotonic correlation, qualification rate,
  AMC national vs peer-group).

## May 11 session - the leaderboard fix iteration

- First leaderboard came back wrong. HOSPITAL PEREA (PR) at 0.997, Keller ACH
  (NY, DoD) at 0.994, KENSINGTON HOSPITAL (PA) at 0.993. None of them
  recognizable, all of them with readm_measures_reported=0 and
  care_measures_reported=1.

- Diagnosed in real-time: a hospital reporting a single high-percentile measure
  gets a domain composite equal to that one rank, and with only one domain
  reported, the top-line equals that single domain composite. One lucky measure
  -> top of leaderboard. Classic small-sample inflation, the same reason CMS
  itself requires minimum measure counts before publishing star ratings.

- Fix: applied minimum-measure thresholds at two levels:
  - Domain composite NULL unless minimum measures reported (>=2 of 4 broad
    HRRP, all 3 HCAHPS stars, >=3 of 4 care measures)
  - Top-line composite NULL unless >=2 qualified domain composites

- Second leaderboard: still showed specialty (cardiac/surgical) hospitals and
  Critical Access dominating the top 10. Same well-documented critique of
  CMS-style composite scoring - case-mix selection effects at small/specialty
  facilities. Validated by checking the CMS overall_rating of the top hospitals
  (positions 2, 4, 5 had CMS 5-star), so the math was correct even though my
  intuition (expecting Mayo/Cleveland/Hopkins at the top) was wrong.

- Lesson: my "where's Mayo?" prediction was based on brand recognition, not on
  what CMS-style metrics actually measure. The leaderboard reflected real CMS
  composite behavior, not a bug. The peer-group ranking column we built handles
  this honestly - within state x hospital_type cohorts, Mayo/Hopkins/Duke jump
  from 60-65th national percentile to 92-99th peer-group percentile. That
  delta IS the case-mix correction story.

## May 12 session - validation, notebook, dashboard

- The temp-view-evaporates-overnight problem. The TEMP VIEW gold_scorecard_dev
  was gone when I came back on May 12 because Databricks temp views are
  session-scoped. Lost ~15 minutes recreating it. Real lesson: persist dev
  artifacts as Delta tables OR commit the SQL to the repo for any multi-day
  iteration. The "lighter" temp view path cost more in the end. Committed
  sql/gold_scorecard_dev_view.sql to the repo afterward so future-me can
  rebuild the view in one paste.

- External validity check came back beautifully: avg composite by CMS rating
  showed strictly monotonic climb 1->2->3->4->5 (0.29, 0.36, 0.42, 0.49, 0.56).
  Each CMS star is worth ~0.066 of our composite. This is the most defensible
  chart in the whole project - an independent composite, built from different
  measures with a different methodology, correlating with CMS's own published
  scoring.

- AMC peer-correction lookup confirmed everything: Mass General +41 percentile
  points after peer-group adjustment, NYU Langone +39, Mayo Clinic (AZ) +36,
  Mayo Rochester +29, Johns Hopkins +31, Duke +30, Cedars-Sinai +33. Cleveland
  Clinic the honest outlier at +13 only, ending at the 57th peer-group
  percentile - likely the most case-mix-heavy quaternary referral center in the
  sample. Documented that openly in the README rather than hiding it.

- Wrapped Block 3.1 by replacing the 04_gold_scorecard scaffold with the real
  implementation: %run ./00_setup for config, _gold_ts and _source_batch_id
  audit columns (matching Silver naming), snapshot_year as soft historical
  dimension included in the MERGE key, liquid clustering on (state,
  hospital_type, snapshot_year), MERGE instead of overwrite, 5 smoke tests
  including monotonic-correlation assertion. All 5 smoke tests passed on first
  run. Built_at timestamp shows the artifact is real, not just a notebook
  that-once-ran.

## May 12 session - Block 3.2 (dashboard)

- Lakeview dashboard UI moved since the scaffold was written. Two-step model
  now: datasets created on a separate Data tab, then visualization tiles on
  the canvas reference those datasets. Built three headline tiles: CMS validity
  bar chart, national Top 25 leaderboard table, AMC national-vs-peer-group
  correction table. Published with shared credentials (right call for a
  portfolio dashboard against public CMS data).

- Captured three screenshots (cms_validity_chart, national_leaderboard,
  peer_group_correction) and embedded them in the README. Visual evidence
  matters - a recruiter scrolling the repo for 30 seconds gets the entire
  story from the embedded images alone.

- Remaining 7 dashboard queries are written and committed to
  sql/dashboard_queries.sql but not yet built as tiles. Will revisit if energy
  allows; the headline trio is enough to drive the recruiter narrative.

## Databricks gotchas resolved this weekend (for future reference)

- TEMP VIEWS are session-scoped. Rebuild on session restart. For multi-day
  iteration, persist as Delta tables or commit the SQL.

- ${VAR} substitution in %sql magic doesn't pick up Python variables on
  serverless (it treats them as :param parameter markers). Workaround:
  spark.sql(f"...{VAR}...") from Python cells. The scaffold's own convention
  - the right pattern to follow.

- Em-dashes (U+2014) from chat paste break the Databricks SQL parser. Use
  plain ASCII hyphens in commit-bound files. Confirmed by writing the SQL
  files locally and running `grep -P "[\x80-\xFF]"` to verify.

- Pasting source-format .py content into an existing notebook cell does NOT
  parse the # COMMAND ---------- separators or # MAGIC %md prefixes. Must
  import the file as a notebook via File -> Import OR drag into the Git
  folder. Cost ~10 minutes to figure out the first time.

- Databricks Free Edition published dashboards still require viewer
  authentication despite "share publicly" wording. Screenshots in the README
  are the real artifact for non-Databricks viewers; the published link is
  the credibility proof.

## Numbers locked from this weekend

- 5,418 hospitals in Silver, 5,418 in Gold (1:1 grain preservation via LEFT
  JOIN on hospital_master)
- 26 DQ checks, 25 passing, 1 SOFT warn (overall_rating null rate, real CMS
  suppression behavior)
- 3,029 hospitals nationally rankable (55.9%) - consistent with CMS's own
  Overall Star Rating publication coverage
- Composite range [0.091, 0.895], mean 0.44, bell-shaped centered on 0.5
  (CLT signature when averaging polarity-aware percentile ranks across
  multiple measures)
- Monotonic CMS correlation: 0.29, 0.36, 0.42, 0.49, 0.56 across 1-2-3-4-5
  star ratings

## What I'd do differently next time

- Persist dev SQL as Delta tables (or commit early) for any iteration that
  spans multiple sessions. The temp-view path felt lighter and cost more.

- Don't trust scaffold column lists - run DESCRIBE first. Saved myself once
  on READM_30_HOSP_WIDE, lost a turn on excess_readmission_ratio guess.

- Anchor leaderboard intuitions on what the metrics actually measure, not
  on brand recognition. "Where's Mayo?" was the wrong question; "is the
  composite monotonic against CMS's own scoring?" was the right one.

- Build the dashboard alongside the Gold layer, not after. Some of the
  validation queries (national-vs-peer-group for named AMCs) would have
  shown the peer-correction story sooner if I'd been visualizing as I
  iterated.

## To do next session(s)

- README polish - embed screenshots, link dashboard, real numbers throughout
  (DONE May 13)
- LinkedIn updates from the Playbook - Featured section, headline, About,
  relaunch post, recommendations asks, push to 500+ connections
- 05_gold_benchmarks notebook - peer-benchmark percentile tables broken by
  region/size/ownership (lower priority - dashboard already shows peer-group
  ranking)
- Remaining 7 dashboard tiles when energy returns
