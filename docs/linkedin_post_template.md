# LinkedIn post templates

Copy, tweak, and post. These are structured for max engagement (hook → story → takeaway → link).

---

## Template A — The honest pivot post (post this Week 1)

> After a decade keeping 4,000+ databases alive as a DBA and SRE — most recently leading SRE for the enterprise DBaaS platform at Citigroup — I've decided to pivot into AI data engineering.
>
> Not because DBA work is going away (it isn't). Because the hard part of production AI turns out to be the same thing I've been doing all along: reliable, well-governed, well-monitored data pipelines.
>
> Over the next 90 days I'm:
> • Finishing AZ-900, DP-900, and DP-300
> • Working toward the Databricks Certified Data Engineer Associate
> • Building a public Databricks lakehouse project on GitHub — starting this weekend
>
> I'll post the progress honestly — including the parts I get stuck on. If you've made a similar pivot, I'd love to hear what you wish you'd known earlier.
>
> #DataEngineering #DatabaseAdministrator #SRE #AzureDatabricks #CareerPivot

---

## Template B — The "I shipped the project" post (post after Weekend 3)

> I spent three weekends building my first Databricks lakehouse project, and I want to share what it taught me.
>
> It's a medallion-style (Bronze → Silver → Gold) pipeline on public CMS hospital quality data — the kind of dataset I worked with as a DBA at PointClickCare in healthcare. The lakehouse lands the raw CSVs, cleans and types them, runs data-quality expectations, and produces a hospital quality scorecard with peer-adjusted benchmarks.
>
> Three things surprised me:
>
> 1) My DBA instincts translated better than I expected. Idempotency, audit columns, partition strategy, the "test your backups actually restore" mindset — all of it maps 1:1 onto lakehouse work. Delta Lake's transaction log is basically `fn_dblog` for data engineers.
>
> 2) The modern data-quality story is still maturing. Great Expectations, Deequ, and Delta Live Tables all solve overlapping problems. For a learning project, rolling my own DQ harness in PySpark was clarifying — I now understand what the tools are buying me.
>
> 3) Z-ordering is real. Partitioning by state + Z-order on (year, hospital_id) took my dashboard queries from 8 seconds to <1 second. Small thing, big user-experience difference.
>
> Repo (README, architecture doc, 6 notebooks, dashboard queries): [link]
>
> Open to feedback, and to conversations with teams building data platforms where regulated-industry reliability is a must-have. Next project: RAG chatbot over the same dataset.
>
> #DataEngineering #Databricks #DeltaLake #Lakehouse #DataQuality

---

## Template C — Short "launch" post (for after dashboard screenshot)

> First Databricks lakehouse — shipped.
>
> Healthcare data (CMS Care Compare), medallion architecture, Delta Lake, ~5,000 US hospitals' quality metrics with peer-adjusted benchmarking.
>
> Link in comments. Feedback welcome.
>
> (And yes, an ex-DBA's first question building this: where's the transaction log. Answer: `DESCRIBE HISTORY`. Lovely.)
>
> [screenshot of dashboard]

---

## Timing guidance

- **Week 1 of the build:** post Template A. This signals intent publicly — useful because it makes you a more "interesting to recruiters" profile even before the repo exists.
- **Week 3 of the build (after shipping):** post Template B with the repo link. This is the one recruiters screenshot and share.
- **Optional, week 4:** post Template C to a different audience (maybe a Databricks subreddit or slack) for reach.

Engagement pattern: the second post (Template B) will outperform the first 5-to-1 because it links to shipped work. Don't be disappointed if Template A gets modest likes — it's the setup, not the punchline.
