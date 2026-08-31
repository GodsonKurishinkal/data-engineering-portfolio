# Enterprise Data Platform

> Production-grade data lakehouse powering supply chain operations at scale

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Polars](https://img.shields.io/badge/Polars-Latest-orange.svg)](https://pola.rs)
[![DuckDB](https://img.shields.io/badge/DuckDB-Latest-yellow.svg)](https://duckdb.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Executive Summary

A **Medallion architecture data lakehouse** processing 30,000+ SKUs daily from 4 source systems (ERP, WMS, OBI, POS). Reduced data latency from 48 hours to 2–4 hours, enabling same-day supply chain decisions.

### Impact at a Glance

| Metric | Before | After | Business Impact |
|--------|--------|-------|-----------------|
| **Processing Time** | 4 hours | 30 minutes | Team can iterate faster on reports |
| **Data Freshness** | 48 hours | 2-4 hours | Same-day inventory decisions |
| **Pipeline Reliability** | ~70% | 95%+ | Fewer morning firefights |
| **Analyst Time on Wrangling** | 60% | 15% | 45% more time for actual analysis |
| **Data Quality Issues** | Discovered in reports | 500+ caught at source | No more "bad data" escalations |

---

## The Problem

### Business Context

The supply chain operations team at a major retail company made decisions on **stale data**. Inventory positions were only knowable after they had already become a problem, and analysts spent most of their week cleaning rather than analysing.

### Technical Pain Points

1. **Fragmented data**: 4 source systems (ERP, WMS, OBI, POS) with no integration
2. **Manual exports**: Daily 4-hour Excel extraction ritual (fragile, error-prone)
3. **No historical tracking**: Only point-in-time snapshots, no trend analysis
4. **Quality discovered late**: Issues found in reports, not at data ingestion
5. **Single point of failure**: One person knew the Excel macros

### What Was at Stake

- **$2M+ inventory decisions** based on 48-hour-old data
- **Stockouts** from delayed visibility into inventory levels
- **Analyst burnout** from repetitive data wrangling

---

## The Solution

### Architecture Choice: Medallion (Bronze/Silver/Gold)

**Why Medallion over alternatives?**

| Alternative | Why Not |
|-------------|---------|
| Single-hop ETL | No recovery from source issues; can't debug transformations |
| Lambda Architecture | Overkill; we don't have real-time requirements |
| Data Vault | Modeling complexity not justified for our scale |
| Delta Lake | Requires Spark; our volume fits single-machine processing |

**Decision documented:** [ADR-003: Medallion Architecture](docs/architecture-decisions/003-medallion-architecture.md)

### Technology Choices

| Component | Technology | Why |
|-----------|------------|-----|
| Processing | Polars | 5-10x faster than Pandas ([ADR-001](docs/architecture-decisions/001-polars-over-pandas.md)) |
| Analytics | DuckDB | Zero-copy Parquet reads ([ADR-002](docs/architecture-decisions/002-duckdb-for-analytics.md)) |
| Storage | Parquet | Columnar, 80% smaller than CSV |
| Extraction | Abstract Base Classes | Reusable patterns for 50+ pipelines |

### Key Implementation Details

**Configuration-driven pipelines** — Adding a new data source is YAML, not code:

```yaml
pipeline:
  name: new_source_daily
  source:
    type: database
    connection: ${NEW_SOURCE_CONN}
    query: "SELECT * FROM table WHERE modified > :last_run"
  destination:
    layer: bronze
    partition_by: [extract_date]
```

**3-tier data quality** — Catch issues early, not in reports:

```
Tier 1: Schema Validation    → Pipeline BLOCKS on failure
Tier 2: Business Rules       → Flags issues, continues pipeline
Tier 3: Statistical Anomalies → Alerts team, logs for review
```

---

## Results & Impact

### Quantified Business Outcomes

| Outcome | Measurement |
|---------|-------------|
| **Same-day reporting enabled** | Previously next-day |
| **Analyst productivity** | +45% time on insights vs. cleaning |
| **Data incidents** | 70% reduction in "bad data" escalations |
| **Quality automation** | 500+ anomalies auto-detected in 12 months |
| **Reusable framework** | 5 additional pipelines built from template |

### Technical Metrics

| Metric | Value |
|--------|-------|
| Pipeline success rate | 98.2% (30-day, see reliability-metrics.md) |
| P95 query latency | < 3 seconds |
| Storage efficiency | 80% savings (Parquet vs CSV) |

---

## What I Learned (Failures Included)

### What Didn't Work

1. **Over-engineering initially**: Started with Airflow + Spark. Killed 2 weeks before realizing Task Scheduler + Polars was simpler and sufficient.

2. **Building in isolation**: Spent 3 weeks on features nobody asked for. Started weekly demos after that.

3. **No monitoring at first**: Silent failures went unnoticed for days. Added observability from day 1 on subsequent projects.

### What I'd Do Differently

- Start with monitoring infrastructure, not ETL code
- Involve stakeholders in schema design (not just final review)
- Budget 30% time for documentation (was 5%, caused painful onboarding)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────────────┤
│     ERP     │     POS     │     WMS     │     OBI     │   External APIs     │
└──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┴──────────┬──────────┘
       │             │             │             │                 │
       ▼             ▼             ▼             ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EXTRACTION LAYER                                      │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐                 │
│  │  DB Conn  │  │  API      │  │  RPA Bot  │  │  File     │                 │
│  │  Extractor│  │  Extractor│  │  Extractor│  │  Extractor│                 │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘                 │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                                      │
│                                                                              │
│  • Raw data landing zone                                                    │
│  • Immutable historical record                                              │
│  • Hive-partitioned Parquet (by extract_date)                              │
│  • Full audit trails                                                        │
│                                                                              │
│  /lakehouse/bronze/{source}/{table}/extract_date={YYYY-MM-DD}/          │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                                      │
│                                                                              │
│  • Cleaned & validated data                                                 │
│  • Schema enforcement                                                       │
│  • Deduplication & null handling                                           │
│  • 3-tier anomaly detection                                                │
│                                                                              │
│  /lakehouse/silver/{domain}/{entity}/                                    │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                        │
│                                                                              │
│  • Business-ready star schema                                               │
│  • 10+ fact tables, 6+ dimension tables                                    │
│  • Optimized for analytical queries                                         │
│  • Powers ML models & dashboards                                           │
│                                                                              │
│  /lakehouse/gold/facts/ & /lakehouse/gold/dimensions/                   │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONSUMPTION LAYER                                     │
├─────────────────┬─────────────────┬─────────────────┬───────────────────────┤
│   Streamlit     │   Power BI      │   ML Models     │   Ad-hoc Analysis     │
│   Dashboards    │   Reports       │   (Forecasting) │   (DuckDB)            │
└─────────────────┴─────────────────┴─────────────────┴───────────────────────┘
```

---

## Project Structure

```
enterprise-data-platform/
├── README.md
├── CONTRIBUTING.md
├── LICENSE
├── Makefile                            # lint / format / clean
├── pyproject.toml
├── architecture/
│   ├── medallion-architecture.md       # Layer specifications
│   └── data-flow.md                    # End-to-end data flow
├── etl_framework/
│   ├── __init__.py
│   ├── extractors/
│   │   ├── base_extractor.py           # Abstract base class
│   │   ├── database_extractor.py       # SQL Server, Oracle
│   │   ├── api_extractor.py            # REST APIs, auth, pagination
│   │   └── rpa_extractor.py            # Selenium / PyAutoGUI bots
│   ├── transformers/
│   │   ├── base_transformer.py         # Abstract base class
│   │   ├── schema_enforcer.py          # Schema specs and registry
│   │   └── data_cleaner.py             # Nulls, dedupe, type coercion
│   └── loaders/
│       ├── parquet_writer.py           # Hive-partitioned Parquet
│       └── duckdb_loader.py            # Analytical query access
├── data_quality/
│   ├── validation_rules/
│   │   └── validation_engine.py        # Tiers 1–2: schema + business rules
│   └── anomaly_detection/
│       └── anomaly_detector.py         # Tier 3: statistical outliers
└── docs/
    ├── architecture-decisions/         # ADR-001 … ADR-007
    ├── performance-benchmarks.md
    ├── reliability-metrics.md
    └── deployment-guide.md
```

## Quick Start

### Prerequisites

```bash
# Python 3.10+
python --version

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

### Installation

```bash
# Install the package and its runtime dependencies
pip install -e .

# Or with the dev tooling (ruff, mypy, black)
pip install -e ".[dev]"

# Verify
python -c "import etl_framework; print(etl_framework.__version__)"
```

### Run a Pipeline

The framework in this repository is the reusable component layer — extractors,
transformers, loaders and the quality engine. The orchestration entry point and
the pipeline YAML configs live in the private production repository; what follows
is how the pieces compose.

```python
from etl_framework.extractors import create_oracle_extractor
from etl_framework.transformers import SchemaEnforcer, INVENTORY_SCHEMA
from etl_framework.loaders import write_to_bronze

extractor = create_oracle_extractor(query="SELECT * FROM vw_inventory_snapshot")
df = extractor.extract()

df = SchemaEnforcer(INVENTORY_SCHEMA).apply(df)
write_to_bronze(df, source="obi", table="inventory", partition_by=["extract_date"])
```

---

## Key Design Decisions

### Why Polars over Pandas?

| Operation | Pandas | Polars | Speedup |
|-----------|--------|--------|---------|
| CSV Read (1GB) | 45s | 8s | **5.6x** |
| GroupBy Agg | 12s | 1.2s | **10x** |
| Join (2 tables) | 8s | 0.9s | **8.9x** |
| Memory Usage | 4GB | 1.1GB | **3.6x** |

### Why Parquet with Hive Partitioning?

- **Columnar storage** → Only read columns you need
- **Compression** → 80% smaller than CSV
- **Partition pruning** → Query only relevant date ranges
- **Schema evolution** → Add columns without breaking readers

### Why Configuration-Driven Pipelines?

```yaml
# New data source? Just add config:
pipelines:
  - name: new_source_daily
    source:
      type: database
      connection: ${NEW_SOURCE_CONN}
      query: "SELECT * FROM table"
    destination:
      layer: bronze
      partition_by: [extract_date]
    schedule: "0 6 * * *"
```

---

## Data Model

### Fact Tables (Gold Layer)

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `fact_sales` | Transaction | Revenue, Quantity, Discount |
| `fact_inventory` | SKU × Location × Day | On-hand, In-transit, Reserved |
| `fact_orders` | Order Line | Order qty, Fulfilled qty, Lead time |
| `fact_replenishment` | SKU × Day | Reorder point, Safety stock, EOQ |
| `fact_forecast` | SKU × Week | Predicted demand, Confidence |

### Dimension Tables

| Table | Attributes |
|-------|------------|
| `dim_product` | SKU, Category, Brand, Supplier |
| `dim_location` | Store, Warehouse, Region, Country |
| `dim_time` | Date, Week, Month, Quarter, Year |
| `dim_supplier` | Supplier, Lead time, MOQ |
| `dim_customer` | Customer segment, Loyalty tier |
| `dim_channel` | Online, Retail, Wholesale |

---

## Data Quality

### 3-Tier Anomaly Detection

```
Tier 1: VALIDATION
├── Schema conformance
├── Required fields
├── Data type enforcement
└── Referential integrity

Tier 2: OUTLIER DETECTION
├── Statistical bounds (IQR, Z-score)
├── Historical range checks
└── Velocity checks (rate of change)

Tier 3: BUSINESS RULES
├── Domain-specific validations
├── Cross-field consistency
└── Temporal logic checks
```

### Results

- **500+ dimension anomalies** identified and fixed
- **70% reduction** in data-related incidents
- **Automated alerting** for critical issues

---

## Performance Benchmarks

| Pipeline | Records/Day | Avg Runtime | P95 Runtime |
|----------|-------------|-------------|-------------|
| Inventory Sync | 2.5M | 12 min | 18 min |
| Sales Transactions | 500K | 8 min | 12 min |
| Replenishment Calc | 10K SKUs | 25 min | 35 min |
| Forecast Generation | 10K SKUs | 45 min | 60 min |

---

## Tech Stack

| Category | Technology |
|----------|------------|
| **Language** | Python 3.10+ |
| **Data Processing** | Polars, DuckDB |
| **Storage Format** | Parquet (Hive-partitioned) |
| **Databases** | SQL Server, Oracle |
| **Automation** | Selenium, PyAutoGUI |
| **Visualization** | Streamlit, Power BI |
| **Orchestration** | Windows Task Scheduler |
| **Version Control** | Git |

---

## License

MIT License - See [LICENSE](LICENSE) for details.

---

## Author

**Godson Kurishinkal Antony**  
Data Engineer | Dubai, UAE

- GitHub: [@GodsonKurishinkal](https://github.com/GodsonKurishinkal)
- LinkedIn: [godsonkurishinkal](https://linkedin.com/in/godsonkurishinkal)
- Portfolio: [godsonkurishinkal.github.io](https://godsonkurishinkal.github.io/data-engineering-portfolio)
