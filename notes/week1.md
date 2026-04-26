# Week 1 Journal — Bronze Ingestion

Built a working medallion lakehouse foundation on Databricks Free Edition with Unity Catalog: idempotent Bronze ingestion from four CMS Care Compare datasets via the stable /datastore/query/ API, three audit columns on every row (_ingest_ts, _source_file, _batch_id), Delta column mapping at the table level. Two transient CMS 503s hit mid-ingest; the idempotency design swallowed them cleanly. That moment alone validated the architecture.

**What surprised me:** I debugged five real production-grade issues live — UC catalog naming, serverless filesystem restrictions, DELTA invalid column characters, blocked session conf, transient HTTP failures. None of these are in tutorials. All of them are on the path.

**What felt familiar (DBA → DE):** audit columns are still audit columns. Idempotency is the same discipline as rerunnable scripts. Delta tables behave like transactional tables with versioning — I already know how to think about ACID.

**What felt foreign:** serverless compute restrictions. As a DBA I'm used to owning the filesystem; Free Edition's UC enforcement blocked the /tmp → dbutils.fs.cp dance, forcing direct UC Volume writes. Better pattern, but it took an hour to accept.

**Next time:** read the UC enforcement docs before writing the first download function. Saves the hour. Bigger lesson: the bugs were the lesson plan. Tutorials teach the happy path; production teaches the rest.
