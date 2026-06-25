# Presentation Notes — 10 Minutes

A timed structure so you don't run over. Practice with a timer once; it's easy to spend 4 minutes
on architecture and rush the data challenges, which is usually the most interesting part to the
panel.

## Timing

| Time | Section | Key point to land |
|---|---|---|
| 0:00–0:45 | Context | Who the "client" is, what problem the platform solves, one sentence on scale |
| 0:45–2:30 | Architecture overview | Walk the diagram left to right, once, without detail |
| 2:30–4:30 | Sources | What you ingested and why each connector type was chosen |
| 4:30–6:30 | Transform & data quality | Bronze→Silver→Gold, name the actual checks, show the quarantine pattern |
| 6:30–7:30 | Data product / output | Who consumes it and what decision it supports |
| 7:30–9:00 | Data challenges | 2–3 concrete ones, with what you changed because of them |
| 9:00–10:00 | Close | One sentence on what you'd improve next, hand to Q&A |

## Speaking notes

**Context (45s)**
"This is a pipeline I built for an online retail business — orders, product catalog, and website
clickstream data feeding daily sales dashboards and customer segmentation. I'll walk through
architecture, then spend most of the time on data quality and the issues that came up, since
that's usually where the real engineering judgment shows."

**Architecture (1:45)**
Point at the diagram, left to right, in one pass: "Three sources land in Data Factory, which
orchestrates ingestion into a raw zone in ADLS. Databricks then runs a Bronze, Silver, Gold
pipeline in PySpark and Delta Lake, and the curated Gold tables feed Power BI and a marketing
feed." Don't explain every box yet — that comes next.

**Sources (2 min)**
Name the three, and *why* each connector type: "Orders came in as a daily file drop — a classic
batch-file source. Customer and product reference data came straight from the CRM's SQL database.
Clickstream came from a REST API, which meant handling pagination and rate limits." This shows
breadth across ADF's three most common connector patterns without needing a fourth source.

**Transform & data quality (2 min) — the core of the talk**
"Bronze is schema-enforced but otherwise untouched — that's the audit trail. Silver is where the
real checks run: not-null on business keys, de-duplication, referential integrity against the
product and customer dimensions, and a value-range check that caught negative quantities. Anything
that fails isn't dropped — it's written to a rejected zone with a reason code, and logged to a
`dq_log` table, so the failure is visible rather than silent." If asked for an example, the
orphaned-product-id case is the clearest one to describe.

**Data product (1 min)**
"Gold has three tables: a daily sales summary the merchandising team uses for their morning
dashboard, a top-products ranking for the weekly trading review, and a customer value table
feeding marketing segmentation." Tie each table to a *person* and a *decision* — that's what makes
it sound like a real data product rather than a table that exists for its own sake.

**Data challenges (1:30) — pick 2, max 3**
Strongest two to lead with:
1. Schema/format drift on the date field — handled with a fallback parse rather than a hard failure.
2. The blocking-quality-gate story — an early version failed the whole pipeline on any reject,
   which took down the next day's dashboard over one bad file; changed to quarantine + alert +
   continue. This one is good because it shows a mistake and a fix, which interviewers like more
   than a clean success story.

**Close (1 min)**
"If I were extending this, the next thing I'd add is [pick one: Unity Catalog for centralized
governance / a proper expectations framework like Delta Live Tables / CDC instead of full reloads
on the dimension tables]. Happy to go deeper on any part of this."

## Likely follow-up questions to be ready for

- "Why Databricks notebooks called from ADF, instead of ADF's own Data Flows?" → Data Flows are
  fine for simpler transforms, but Spark notebooks give more control over the quality-check logic
  and are easier to unit-test and version in git alongside the rest of the code.
- "How would you handle schema evolution if a new column gets added upstream?" → `mergeSchema` on
  Delta writes for additive changes; anything that changes an existing column's type should still
  fail loudly rather than silently coerce.
- "How do you avoid reprocessing the whole table every run?" → mention incremental load patterns
  (watermark column / `ingestion_date` partition pruning), even if this demo uses a full daily
  reload for simplicity.
- "What would break this at 100x the data volume?" → single-node-ish job cluster sizing and the
  `dropDuplicates` full-shuffle in Silver would need autoscaling clusters and a partitioning
  strategy on `order_date`.
