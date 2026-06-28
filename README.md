# Retail Sales & Customer Insight Pipeline

An Azure Data Factory + Azure Databricks pipeline demonstrating a medallion-architecture
(Bronze/Silver/Gold) data platform for a retail sales and customer analytics use case, with
data-quality enforcement at every layer.

![Architecture](docs/architecture-diagram.svg)

> **About this repo.** This was built and validated end-to-end on a live Azure free-tier
> subscription: a real ADLS Gen2 account, a real ADF pipeline, and real Databricks notebooks run
> on Serverless compute against real (synthetic) sample data. It isn't a client engagement —
> it's a self-driven build to demonstrate the full pattern hands-on. The numbers and bugs
> described below are what actually happened during the build, not a theoretical design.

---

## 1. What this covers

| Topic | Where |
|---|---|
| Sources | [Section 3](#3-sources) |
| Transfer & data quality checks | [Section 4](#4-pipeline--data-quality) |
| Data products / outputs | [Section 5](#5-data-products) |
| Real challenges hit during the build | [Section 6](#6-data-challenges) |
| How to rebuild this yourself on the free tier | [Section 7](#7-building-this-yourself-on-the-azure-free-tier) |
| Unit tests / TDD | [Section 8](#8-testing) |

A 10-minute presentation script is in [`docs/presentation-notes.md`](docs/presentation-notes.md).

## 2. Architecture

Sources → **Azure Data Factory** (orchestration & ingestion) → **ADLS Gen2 raw zone** →
**Azure Databricks** (Bronze → Silver → Gold, PySpark + Delta Lake, on Serverless compute) →
**Unity Catalog managed tables** → Power BI / downstream consumers.

- **Bronze**: raw data landed as-is with schema enforcement and ingestion metadata. Append-only.
- **Silver**: data-quality checks applied (not-null, de-duplication, value-range, referential
  integrity). Failing rows are quarantined with a reason code, not silently dropped.
- **Gold**: business aggregates, registered as Unity Catalog tables with **Liquid Clustering**.

**Validated counts from an actual run:** 21 raw order rows in → 6 rejected (≈29%, across 5
overlapping checks — see [Section 6](#6-data-challenges) for why the rejected count and the
"removed from clean data" count aren't quite the same number) → 16 clean orders in Silver → 3
Gold tables built on top.

## 3. Sources

Three source types were used to cover the connector patterns ADF is commonly used for. The
**orders source (file-based) is fully live** — real ADF Copy activity, real ADLS containers. The
**SQL Database and REST API sources are designed and documented** (linked services, datasets,
pipelines in `adf/`) but weren't provisioned live in this build — worth being upfront about if
asked to demo them specifically.

| Source | System type | Status |
|---|---|---|
| Order/transaction extracts | Daily file drop, Blob → Copy Activity | **Live** — ran end to end |
| Customer & product catalog | Azure SQL Database connector | Designed, not provisioned live |
| Web & app clickstream | REST API connector (paginated) | Designed, not provisioned live |

Sample files are in [`data/sample/`](data/sample/). The orders file has several intentional
data-quality issues baked in — a null key, a duplicate row, an orphaned product reference, a
negative quantity, a malformed date — used to validate that the Silver checks actually catch them.

## 4. Pipeline & Data Quality

**ADF (`adf/pipelines/`):** `pl_ingest_orders` runs a `GetMetadata` existence/non-empty check
before a Copy activity lands the file in `raw`. The fuller orchestration
(`pl_master_orchestration`, with the Databricks notebook activities and a reject-rate alert) is
designed in the repo as the next step once the file-based pipeline is proven.

**Databricks (`databricks/notebooks/`), run on Serverless compute:**

1. **`01_bronze_ingest_validate`** — schema-enforced landing into Delta, with `_ingest_ts` /
   `_source_file` lineage columns (using `_metadata.file_path`, not the legacy
   `input_file_name()` — see Section 6).
2. **`02_silver_clean_transform`** — runs five checks, each one a small, separately-commented
   function: not-null on business keys, de-duplication, negative-value check, and two
   referential-integrity checks (orphaned `customer_id` and `product_id`). Every rejected row is
   tagged with a reason code and written to the `rejected` external location.
3. **`03_gold_aggregate`** — builds the three Gold tables, clustered by the columns each
   table is actually filtered/grouped by downstream.

**Connecting Databricks to ADLS** doesn't use a mount or an account key — see Section 6 for why —
it uses a Unity Catalog **External Location**, backed by a **Storage Credential** referencing an
**Access Connector**'s managed identity. No secret lives in any notebook.

## 5. Data Products

| Gold table | Clustered by | Used by | Purpose |
|---|---|---|---|
| `retail_gold.daily_sales_summary` | `order_date`, `region` | Power BI ops dashboard | Daily trading view by region/category |
| `retail_gold.top_products` | `category` | Weekly merchandising review | "What's moving" ranking by revenue/units |
| `retail_gold.customer_value` | `customer_segment` | Marketing/CRM | Lifetime value, recency, for segmentation |

## 6. Data Challenges

These are real issues hit and fixed during this build, not invented ones:

- **DBFS mounts don't work on Serverless compute.** The original design used
  `dbutils.fs.mount` with a storage account key. Serverless compute (the default for new
  Unity-Catalog workspaces) doesn't support mounts or raw account-key Spark configs at all.
  **Fix:** Unity Catalog External Locations, authenticated via an Access Connector's managed
  identity — no key or secret anywhere in the code.
- **`input_file_name()` is blocked under Unity Catalog** (`UC_COMMAND_NOT_SUPPORTED`) — it can
  expose file paths outside the governed access model. **Fix:** use the `_metadata.file_path`
  column instead, which Unity Catalog adds automatically to file-based reads.
- **`to_date()` raises instead of returning null on a format mismatch, under ANSI mode** (the
  current Databricks default). The original fallback logic — `coalesce()` over two `to_date()`
  calls to handle a `DD-MM-YYYY` vs `YYYY-MM-DD` format inconsistency — assumed the older
  "fail silently with null" behaviour and broke immediately on the first malformed date.
  **Fix:** `try_to_date()`, the ANSI-safe equivalent that returns `NULL` on a parse failure.
- **The "6 rejected" count and "5 fewer clean rows" count don't match, and that's correct, not a
  bug.** One seeded duplicate order (`O1011`) appears twice in the raw data; the duplicate check
  logs **both** occurrences to the rejected/audit table (so the full duplicate pair is visible
  for review) but `dropDuplicates()` only removes one of the two from the working dataset. Worth
  being able to explain this distinction if asked to reconcile the numbers.
- **Orphaned foreign keys** — an order referencing a `product_id` (`P099`) that isn't in the
  catalog, and an order referencing a `customer_id` (`C016`) that isn't in the customer table.
  Caught by the referential-integrity checks and quarantined rather than silently joined as null.
- **A blocking quality gate would have caused a worse outage than the bad data itself.** Designed
  (not yet load-tested live): rather than failing the whole pipeline on any reject, the design
  quarantines, logs, and alerts past a 5% threshold — but lets Gold still build on the clean
  subset. Good "what would you do differently" material if asked.

## 7. Building this yourself on the Azure free tier

1. **Resource group, storage, ADF, Databricks workspace** — see the step-by-step Azure setup
   walkthrough (resource group → ADLS Gen2 with hierarchical namespace → 5 containers → ADF
   instance → Databricks workspace) — straightforward portal steps, no special gotchas.
2. **Connect Databricks to ADLS the Unity Catalog way** (this is the part with real gotchas,
   detailed in Section 6):
   - Create an **Access Connector for Azure Databricks** (a managed identity resource).
   - Grant it **Storage Blob Data Contributor** on the storage account via IAM.
   - In Databricks: Catalog → Create a **Storage Credential** (Azure Managed Identity type),
     referencing the Access Connector's resource ID.
   - Create one **External Location** per container the notebooks touch (`raw`, `processed`,
     `rejected`), each pointing at `abfss://<container>@<account>.dfs.core.windows.net/` using
     that storage credential.
3. **Run the notebooks** in order on Serverless compute: `01_bronze_ingest_validate` →
   `02_silver_clean_transform` → `03_gold_aggregate`. Use **Run all** rather than re-running
   individual cells out of order — notebook variables persist across cells, and re-running a
   cell that reassigns `orders` partway through a chain of checks will silently work against
   already-cleaned data instead of the original raw rows.
4. **Cost control**: this all fits comfortably inside the free-tier credit and Serverless'
   pay-per-use model for a demo of this size. No always-on cluster to forget about.

## 8. Testing

The four data-quality check functions are extracted into [`src/quality_checks.py`](src/quality_checks.py)
specifically so they're unit-testable locally with pytest and a local SparkSession — no Databricks
cluster needed to verify the logic.

```
pip install -r requirements-dev.txt
pytest tests/ -v
```

`tests/test_quality_checks.py` covers the four checks already used in the live Silver notebook
(characterization tests, written after the logic, as a regression safety net).

`tests/test_check_future_dates.py` covers a check added with genuine TDD — written and run against
the function *before it existed* (confirmed failing with `ImportError`), then implemented to make
the tests pass, then reviewed for refactoring (none needed — the implementation was already
minimal). `check_future_dates` itself isn't yet wired into the live Silver notebook; it exists here
as a tested, ready-to-use addition.

---

## Repo structure

```
retail-databricks-adf-pipeline/
├── README.md
├── conftest.py                    # pytest fixtures (local SparkSession, src/ on path)
├── requirements-dev.txt
├── docs/
│   ├── architecture-diagram.svg
│   └── presentation-notes.md
├── data/sample/                  # synthetic sample data, incl. seeded DQ issues
├── src/
│   └── quality_checks.py         # testable quality-check functions
├── tests/
│   ├── test_quality_checks.py
│   └── test_check_future_dates.py  # built with genuine TDD - see Section 8
├── adf/
│   ├── linkedServices/
│   ├── datasets/
│   ├── pipelines/
│   └── triggers/
└── databricks/notebooks/         # 01_bronze, 02_silver, 03_gold - validated end to end
```
