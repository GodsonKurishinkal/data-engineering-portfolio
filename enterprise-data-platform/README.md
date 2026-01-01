# Enterprise Data Platform

> Production-grade data lakehouse powering supply chain operations at scale

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Polars](https://img.shields.io/badge/Polars-Latest-orange.svg)](https://pola.rs)
[![DuckDB](https://img.shields.io/badge/DuckDB-Latest-yellow.svg)](https://duckdb.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🎯 Overview

A **Medallion architecture data lakehouse** designed for enterprise retail supply chain operations. This platform processes data from multiple source systems (ERP, CRM, WMS, OBI) and delivers analytics-ready datasets for demand forecasting, inventory optimization, and operational reporting.

### Key Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| ETL Processing Time | 4 hours | 30 minutes | **87%** ⬇️ |
| Data Freshness | 48 hours | 2-4 hours | **90%** ⬇️ |
| Pipeline Reliability | ~70% | 95%+ | **25%** ⬆️ |
| Data Quality Issues | Unknown | 500+ caught | **Automated** |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────────────┤
│     ERP     │     CRM     │     WMS     │     OBI     │   External APIs     │
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
│                         🥉 BRONZE LAYER                                      │
│                                                                              │
│  • Raw data landing zone                                                    │
│  • Immutable historical record                                              │
│  • Hive-partitioned Parquet (by extract_date)                              │
│  • Full audit trails                                                        │
│                                                                              │
│  📁 /lakehouse/bronze/{source}/{table}/extract_date={YYYY-MM-DD}/          │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         🥈 SILVER LAYER                                      │
│                                                                              │
│  • Cleaned & validated data                                                 │
│  • Schema enforcement                                                       │
│  • Deduplication & null handling                                           │
│  • 3-tier anomaly detection                                                │
│                                                                              │
│  📁 /lakehouse/silver/{domain}/{entity}/                                    │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         🥇 GOLD LAYER                                        │
│                                                                              │
│  • Business-ready star schema                                               │
│  • 15+ fact tables, 6+ dimension tables                                    │
│  • Optimized for analytical queries                                         │
│  • Powers ML models & dashboards                                           │
│                                                                              │
│  📁 /lakehouse/gold/facts/ & /lakehouse/gold/dimensions/                   │
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

## 📁 Project Structure

```
enterprise-data-platform/
├── README.md                    # This file
├── architecture/
│   ├── medallion-architecture.md    # Detailed layer specifications
│   ├── data-flow.md                 # End-to-end data flow documentation
│   └── system-integration-diagram.png
├── etl-framework/
│   ├── __init__.py
│   ├── config/
│   │   ├── pipeline_config.yaml     # Pipeline definitions
│   │   └── source_config.yaml       # Source system configs
│   ├── extractors/
│   │   ├── base_extractor.py        # Abstract base class
│   │   ├── database_extractor.py    # SQL Server, Oracle
│   │   ├── api_extractor.py         # REST APIs
│   │   └── rpa_extractor.py         # Selenium/PyAutoGUI bots
│   ├── transformers/
│   │   ├── base_transformer.py      # Abstract base class
│   │   ├── cleaner.py               # Data cleaning operations
│   │   ├── validator.py             # Schema validation
│   │   └── enricher.py              # Data enrichment
│   └── loaders/
│       ├── base_loader.py           # Abstract base class
│       └── parquet_loader.py        # Hive-partitioned Parquet
├── orchestration/
│   ├── task-scheduler-configs/      # Windows Task Scheduler XMLs
│   └── batch-scripts/               # Orchestration batch files
├── data-quality/
│   ├── validation-rules/            # Business rule definitions
│   └── anomaly-detection/           # 3-tier detection system
└── docs/
    ├── performance-benchmarks.md    # Speed comparisons
    ├── reliability-metrics.md       # SLA tracking
    └── deployment-guide.md          # Production setup
```

---

## 🚀 Quick Start

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
# Install dependencies
pip install -r requirements.txt

# Verify installation
python -c "import polars; import duckdb; print('Ready!')"
```

### Run a Pipeline

```bash
# Execute a single pipeline
python -m etl_framework.run --pipeline inventory_daily

# Execute all pipelines for a source
python -m etl_framework.run --source erp

# Dry run (no writes)
python -m etl_framework.run --pipeline inventory_daily --dry-run
```

---

## 💡 Key Design Decisions

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

## 📊 Data Model

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

## 🔒 Data Quality

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

## 📈 Performance Benchmarks

| Pipeline | Records/Day | Avg Runtime | P95 Runtime |
|----------|-------------|-------------|-------------|
| Inventory Sync | 2.5M | 12 min | 18 min |
| Sales Transactions | 500K | 8 min | 12 min |
| Replenishment Calc | 10K SKUs | 25 min | 35 min |
| Forecast Generation | 10K SKUs | 45 min | 60 min |

---

## 🛠️ Tech Stack

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

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---

## 👤 Author

**Godson Kurishinkal**  
Senior Data Engineer | Dubai, UAE

- GitHub: [@GodsonKurishinkal](https://github.com/GodsonKurishinkal)
- LinkedIn: [godsonkurishinkal](https://linkedin.com/in/godsonkurishinkal)
- Portfolio: [godsonkurishinkal.github.io](https://godsonkurishinkal.github.io/data-engineering-portfolio)
