# Retail Supply Chain & Inventory Analytics Platform

A production-grade data engineering project built on **Azure Databricks** with **Unity Catalog** and **Delta Lake**, implementing a full medallion architecture (Bronze → Silver → Gold) for a retail supply chain and inventory domain.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Catalog Structure](#catalog-structure)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Utilities](#utilities)
- [Configuration](#configuration)
- [Setup & Run Order](#setup--run-order)
- [Pipeline Patterns](#pipeline-patterns)
- [Design Decisions](#design-decisions)

---

## Overview

This project ingests retail data from multiple sources (POS transactions, warehouse inventory, store inventory, purchase orders, and master data) and processes it through a three-layer medallion architecture to produce BI-ready gold tables connected to Power BI reports.

**Domain:** Retail Supply Chain & Inventory  
**Platform:** Azure Databricks, Unity Catalog, Delta Lake  
**Catalog:** `azure3_team7_project`

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  POS Stream  │  POS Batch  │  Warehouse  │  Store Inv  │  ...  │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Autoloader (cloudFiles)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER                              │
│  Raw ingestion — append-only, no transformation, exact copy     │
│  Audit cols: source_file, ingested_at, pipeline_run_id          │
└──────────────────────────────┬──────────────────────────────────┘
                               │  spark.read + MERGE using audit watermark (for batch)
                               │  subscribe to Kafka Topic + foreachBatch (for stream)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                              │
│  Cleaned, typed, deduplicated, merged                           │
│  SCD Type 1 (snapshot tables) + SCD Type 2 (master tables)     │
│  Audit cols: ingested_at, processed_at, pipeline_run_id         │
└──────────────────────────────┬──────────────────────────────────┘
                               │  spark.read + full recompute
                               │  spark.read + audit watermark
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                               │
│  Aggregated, enriched, BI-ready                                 │
│  Dimension tables + Fact tables                                 │
│  Connected to Power BI reports                                  │
│  Audit cols: _gold_processed_at, pipeline_run_id                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Compute | Azure Databricks |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Catalog & Governance | Unity Catalog |
| Processing Engine | Apache Spark (PySpark) |
| Reporting | Power BI |
| Orchestration | Apache Airflow (planned) |
| Version Control | Git |

---

## Project Structure

```
mini_project_a3t7/
│
├── config/
│   └── config.json                          # Central config — all table names, paths, settings
│
├── utils/
│   ├── audit.py                                # start_audit, end_audit, get_last_successful_run_time
│   ├── schema_registry.py                      # All bronze, silver, gold schemas — single source of truth
│   └── quality_checks.py                       # Data quality assertion helpers
│   └── maintenance.ipynb                       # OPTIMIZE + VACUUM + ZORDER all tables
│
├── setup/
│   ├── 00_setup_catalog_schemas_volumes.ipynb  # Create catalog, schemas, volumes
│   ├── 01_create_audit_table.ipynb             # Create platform.pipeline_audit
│   └── 02_create_tables.ipynb                  # Create all bronze + silver + gold tables
│
├── ingestion/                               # Bronze notebooks
│   ├── bronze_pos_transactions_stream.ipynb
│   ├── bronze_pos_transactions_batch.ipynb
│   ├── bronze_warehouse_inventory.ipynb
│   ├── bronze_store_inventory.ipynb
│   ├── bronze_purchase_orders.ipynb
│   ├── bronze_customers.ipynb
│   ├── bronze_product_master.ipynb
│   ├── bronze_store_master.ipynb
│   └── bronze_supplier_master.ipynb
│
├── transformation/                          # Silver notebooks
│   ├── silver_pos_transactions_stream.ipynb
│   ├── silver_pos_transactions_batch.ipynb
│   ├── silver_warehouse_inventory.ipynb
│   ├── silver_store_inventory.ipynb
│   ├── silver_purchase_orders.ipynb
│   ├── silver_customers.ipynb
│   ├── silver_product_master.ipynb
│   ├── silver_store_master.ipynb
│   └── silver_supplier_master.ipynb
│
└── aggregation/                             # Gold notebooks
    ├── gold_dim_product.ipynb
    ├── gold_dim_store.ipynb
    ├── gold_dim_supplier.ipynb
    ├── gold_dim_customer.ipynb
    ├── gold_fact_inventory_full.ipynb
    ├── gold_fact_inventory_kpis.ipynb
    ├── gold_fact_daily_inventory.ipynb
    ├── gold_fact_demand_trends.ipynb
    ├── gold_fact_customer_sales.ipynb
    └── gold_fact_supplier_performance.ipynb
```

---

## Data Sources

| Source | Format | Frequency | Volume | Bronze Table |
|---|---|---|---|---|
| POS Transactions (Stream) | JSONL | Real-time | High | `bronze.pos_transactions` |
| POS Transactions (Batch) | CSV | Daily | ~100K rows/day | `bronze.pos_transactions` |
| Warehouse Inventory | Parquet | Hourly | ~50K rows | `bronze.warehouse_inventory` |
| Store Inventory | JSONL | Every 15 min | ~25K rows | `bronze.store_inventory` |
| Purchase Orders | CSV (nested JSON) | Daily + CDC | ~100K rows | `bronze.purchase_orders` |
| Customers | CSV | Daily | ~100K rows | `bronze.customers` |
| Product Master | Parquet | Weekly + incremental | ~50K rows | `bronze.product_master` |
| Store Master | CSV | Weekly + incremental | ~500 rows | `bronze.store_master` |
| Supplier Master | CSV | Weekly + incremental | ~200 rows | `bronze.supplier_master` |

All source files land in Databricks Volumes at:
```
/Volumes/azure3_team7_project/bronze/landing/<entity>/
```

---

## Catalog Structure

```
azure3_team7_project
│
├── bronze                    # Raw ingestion — append-only
│   ├── pos_transactions      # POS stream + batch (distinguished by _source col)
│   ├── warehouse_inventory
│   ├── store_inventory
│   ├── purchase_orders
│   ├── customers
│   ├── product_master
│   ├── store_master
│   └── supplier_master
│
├── silver                    # Cleaned, typed, merged
│   ├── pos_transactions      # Deduplicated, stream + batch reconciled
│   ├── warehouse_inventory   # Latest snapshot per warehouse+product
│   ├── store_inventory       # Latest snapshot per store+product
│   ├── purchase_orders       # CDC merged on po_id
│   ├── customers             # SCD Type 1 merged on customer_id
│   ├── product_master        # SCD Type 2 — full version history
│   ├── store_master          # SCD Type 2 — full version history
│   └── supplier_master       # SCD Type 2 — full version history
│
├── gold                      # Aggregated, BI-ready
│   ├── dim_product
│   ├── dim_store
│   ├── dim_supplier
│   ├── dim_customer
│   ├── dim_date              # Calendar dimension (static, generated once)
│   ├── fact_inventory_full   # POS + warehouse + product — daily grain
│   ├── fact_inventory_kpis   # Rolling 30d/90d KPIs on top of fact_inventory_full
│   ├── fact_daily_inventory  # POS + warehouse + product + store shelf — daily grain
│   ├── fact_demand_trends    # Rolling demand analytics on top of fact_daily_inventory
│   ├── fact_customer_sales   # Enriched transactions with customer attributes
│   └── fact_supplier_performance # Supplier KPIs across all PO history
│
└── platform
    └── pipeline_audit        # Central audit log — every pipeline run recorded here
```

---

## Bronze Layer

**Rule: Accept everything, reject nothing, transform nothing.**

Every bronze table is append-only. Source data is stored exactly as received. All business logic is deferred to silver.

### Ingestion Pattern

All sources use **Databricks Autoloader** (`cloudFiles` format) with `trigger(availableNow=True)` for incremental batch-style processing:

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")         # or parquet, json
    .option("cloudFiles.schemaLocation", ...)
    .schema(BRONZE_SCHEMA)
    .load(SOURCE_PATH)
```

### Audit Columns (added by every bronze notebook)

| Column | Type | Description |
|---|---|---|
| `source_file` | String | Full path of source file (`_metadata.file_path`) |
| `ingested_at` | Timestamp | When row landed in bronze |
| `ingested_date` | String | Partition column, derived from `ingested_at` |
| `pipeline_run_id` | String | UUID linking to `platform.pipeline_audit` |

### Special: POS Transactions

POS transactions have two sources landing in the same bronze table:

```
_source = 'pos_realtime_stream'  ← JSONL stream (continuous)
_source = 'pos_batch_csv'        ← Daily CSV batch (2 AM)
```

The `_source` column is used by silver notebooks to route processing correctly.

---

## Silver Layer

**Rule: Clean, type, deduplicate, merge. Enforce business rules.**

Three transformation patterns used:

### Pattern 1 — SCD Type 1 (Current State)

Used for: `customers`, `warehouse_inventory`, `store_inventory`, `purchase_orders`

Latest state wins. Old values overwritten on update. MERGE on business key.

```
spark.read + audit watermark → transform → MERGE on key
```

### Pattern 2 — SCD Type 2 (Full History)

Used for: `product_master`, `store_master`, `supplier_master`

Every version of a record is kept. `is_current=True` marks the latest version. `scd_hash` (MD5 of tracked columns) detects changes.

```
spark.read + audit watermark → compute scd_hash →
classify NEW/CHANGED/SAME → single atomic MERGE
(close old version + insert new version in one transaction)
```

### Pattern 3 — Stream + Batch Reconciliation

Used for: `pos_transactions` only

```
JOB A (continuous): stream → silver (INSERT only, 24/7)
JOB B (daily 2AM) : batch → silver (INSERT + UPDATE + DELETE cancelled)
```

### Production Pattern

All silver notebooks (except POS stream) use `spark.read` + audit table watermark instead of streaming checkpoints:

```python
last_run_time = get_last_successful_run_time(spark, PROJECT_ROOT, PIPELINE)

if last_run_time:
    df = spark.read.table(SOURCE).filter(col("ingested_at") > lit(last_run_time))
else:
    df = spark.read.table(SOURCE)  # first run — process everything
```

Watermark stored in `platform.pipeline_audit` — fully transparent, reprocessable by updating a single row.

### Audit Columns

| Column | Type | Description |
|---|---|---|
| `ingested_at` | Timestamp | Carried from bronze |
| `processed_at` | Timestamp | When row was processed in silver |
| `pipeline_run_id` | String | UUID linking to `platform.pipeline_audit` |
| `source_system` | String | Origin system name |

---

## Gold Layer

**Rule: Aggregate, enrich, optimise for BI queries.**

### Dimension Tables

| Table | Source | Refresh | Write Mode |
|---|---|---|---|
| `dim_product` | `silver.product_master` (is_current=True) | Weekly | Full overwrite |
| `dim_store` | `silver.store_master` (is_current=True) | Weekly | Full overwrite |
| `dim_supplier` | `silver.supplier_master` (is_current=True) | Weekly | Full overwrite |
| `dim_customer` | `silver.customers` | Daily | MERGE on customer_id |
| `dim_date` | Generated | Once | Full overwrite |

### Fact Tables (PBI Connected)

| Table | Sources | Refresh | Write Mode | PBI Report |
|---|---|---|---|---|
| `fact_inventory_full` | pos_transactions + warehouse_inventory + product_master | Daily | Dynamic partition overwrite | — |
| `fact_inventory_kpis` | fact_inventory_full | Daily | Dynamic partition overwrite | inventory_kpis1 |
| `fact_daily_inventory` | pos_transactions + warehouse_inventory + product_master + store_inventory | Daily | Dynamic partition overwrite | — |
| `fact_demand_trends` | fact_daily_inventory | Daily | Full overwrite by year_month | demand_intelligence |
| `fact_customer_sales` | pos_transactions + customers | Daily | Dynamic partition overwrite | fact_customer_sales |
| `fact_supplier_performance` | purchase_orders | Daily | Full overwrite | supplier_performance_fact |

### Gold Dependency Chain

```
silver tables
    │
    ├── gold_fact_inventory_full  ──→  gold_fact_inventory_kpis
    │
    ├── gold_fact_daily_inventory ──→  gold_fact_demand_trends
    │
    ├── gold_fact_customer_sales
    │
    └── gold_fact_supplier_performance
```

### Audit Columns

| Column | Type | Description |
|---|---|---|
| `_gold_processed_at` | Timestamp | When gold row was computed |
| `pipeline_run_id` | String | UUID linking to `platform.pipeline_audit` |
| `_fact_processed_at` | Timestamp | Used in `fact_customer_sales` (matches PBI schema) |

---

## Utilities

### `utils/audit.py`

Central audit logging for all pipelines.

```python
# Start a pipeline run — returns run_id
run_id = start_audit(spark, PROJECT_ROOT, pipeline_name, target_table, source_table)

# End a pipeline run — record status and metrics
end_audit(spark, PROJECT_ROOT, run_id, "SUCCESS",
          rows_read=1000, rows_written=950,
          extra={"rows_inserted": "800", "rows_updated": "150"})

# Get watermark for incremental reads
last_run_time = get_last_successful_run_time(spark, PROJECT_ROOT, pipeline_name)
```

Every pipeline run is recorded in `platform.pipeline_audit` with:
`run_id`, `pipeline_name`, `source_table`, `target_table`, `start_time`, `end_time`, `rows_read`, `rows_written`, `rows_rejected`, `status`, `error_message`, `extra_metadata`

### `utils/schema_registry.py`

Single source of truth for all table schemas. Every notebook imports its schema from here — no column definitions in notebooks.

```python
from utils.schema_registry import BRONZE_CUSTOMERS_SCHEMA
from utils.schema_registry import SILVER_PRODUCT_MASTER_SCHEMA
from utils.schema_registry import GOLD_FACT_INVENTORY_KPIS_SCHEMA
```

Schemas defined for: 8 bronze tables, 8 silver tables, 4 dim tables, 6 fact tables.

### `utils/quality_checks.py`

Helper functions for data quality assertions used in notebooks.

---

## Configuration

All table names, file paths, checkpoint locations, and pipeline settings are centralised in `config/config.json`. No hardcoded values in notebooks.

```json
{
    "catalog": "azure3_team7_project",
    "schemas": {
        "bronze": "azure3_team7_project.bronze",
        "silver": "azure3_team7_project.silver",
        "gold":   "azure3_team7_project.gold",
        "platform": "azure3_team7_project.platform"
    },
    "tables": {
        "bronze_customers":              "azure3_team7_project.bronze.customers",
        "silver_customers":              "azure3_team7_project.silver.customers",
        "gold_fact_customer_sales":      "azure3_team7_project.gold.fact_customer_sales",
        "pipeline_audit":                "azure3_team7_project.platform.pipeline_audit"
    },
    "landing_paths": {
        "customers": "/Volumes/azure3_team7_project/bronze/landing/customers/"
    },
    "checkpoint_paths": {
        "bronze_customers":                    "/Volumes/azure3_team7_project/bronze/checkpoints/customers/",
        "silver_pos_transactions_stream":      "/Volumes/azure3_team7_project/silver/checkpoints/pos_stream/"
    },
    "pipeline": {
        "vacuum_retain_hours": 168,
        "watermark_hours": 2
    }
}
```

### Dynamic Project Root

Every notebook derives its project root dynamically — no hardcoded workspace paths:

```python
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace" + _notebook_path.split("/mini_project_a3t7/")[0] + "/mini_project_a3t7"
```

Works on any Databricks workspace as long as the git folder is named `mini_project_a3t7`.

---

## Setup & Run Order

### First Time Setup

```
1. setup/00_setup_catalog_schemas_volumes.ipynb   # Create catalog, schemas, volumes
2. setup/01_create_audit_table.ipynb              # Create platform.pipeline_audit
3. setup/02_create_tables.ipynb                   # Create bronze + silver + gold tables
```

### Pipeline Schedules

| Pipeline | Schedule |
|---|---|
| POS stream (JOB A) | Continuous 24/7 |
| Daily batch (JOB B) | 2 AM daily |
| Warehouse inventory | Every hour |
| Store inventory | Every 15 minutes |
| Weekly (Product, Store, Supplier) masters | Sunday midnight |
| Maintenance | Nightly after daily gold |

---

## Pipeline Patterns

### Incremental Processing

All batch silver notebooks use `spark.read` + audit table watermark. No streaming checkpoints for batch sources. Watermark is transparent, queryable, and reprocessable.

```python
# Reprocess from scratch:
# DELETE FROM platform.pipeline_audit WHERE pipeline_name = 'silver_customers'

# Reprocess from specific date:
# UPDATE platform.pipeline_audit
# SET end_time = '2024-01-15' WHERE pipeline_name = 'silver_customers'
# AND status = 'SUCCESS' ORDER BY end_time DESC LIMIT 1
```

### Idempotency

Every notebook is safe to rerun:
- Bronze: Autoloader checkpoint prevents reprocessing same files
- Silver: MERGE on business key — same input = same output
- Gold: Overwrite mode — reruns produce identical results

### Late Data Handling

Event time (`transaction_ts`) used for partitioning, not arrival time (`ingested_at`). Late arriving events land in the correct historical partition automatically.

### SCD2 Atomicity

Master table SCD2 updates use a single MERGE transaction — closes old version AND inserts new version atomically. No broken state possible if pipeline fails mid-execution.

---

## Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Ingestion batch pattern | Autoloader (`iaAvailable = True`) | Incremental, exactly-once, schema evolution |
| Ingestion stream pattern | Subscribe to Kafka Topic | Incremental, exactly-once, schema evolution |
| Table format | Delta Lake | ACID transactions, time travel, MERGE support |
| Watermark storage | Audit table | Transparent, queryable, reprocessable vs opaque checkpoint files |
| SCD2 implementation | Single atomic MERGE | No broken state — both close + insert in one transaction |
| Silver batch pattern | spark.read + audit watermark | Production-grade, operationally flexible |
| Silver stream pattern | foreachBatch | Only correct choice for true streaming source |
| Gold write mode (facts) | Dynamic partition overwrite | Rewrites only affected date partitions |
| Gold write mode (dims) | Full overwrite | Small tables, always current state |
| Gold write mode (supplier) | Full overwrite | Aggregate across all history, 200 rows |
| Schema management | Schema registry | Single source of truth, no duplication |
| Config management | config.json | No hardcoded paths or table names in notebooks |

---

## Notes

- `fact_inventory_full` is an intermediate gold table feeding `fact_inventory_kpis`
- `fact_daily_inventory` is an intermediate gold table feeding `fact_demand_trends` 
- POS stream (JOB A) must be stopped before JOB B (batch) runs at 2 AM and restarted after
- `dim_date` covers 2020-01-01 to 2030-12-31 — extend range in `02_create_tables.ipynb` if needed
- Maintenance notebook runs OPTIMIZE + VACUUM with 168-hour retention (7 days time travel)
