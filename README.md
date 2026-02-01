# Godson Kurishinkal | Senior Data Engineer

> Building production data infrastructure that powers supply chain decisions for 10,000+ SKUs daily.
> **Not dashboards. Not reports. Production-grade data systems.**

[![Portfolio](https://img.shields.io/badge/Portfolio-Live-success?style=for-the-badge)](https://godsonkurishinkal.github.io/data-engineering-portfolio/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin)](https://linkedin.com/in/godsonkurishinkal)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?style=for-the-badge&logo=gmail)](mailto:godson.kurishinkal@gmail.com)

---

## About

**Senior Data Engineer** at **Landmark Group**, Dubai, UAE.

I design and build data platforms that solve real business problems. Not proof-of-concepts—production systems that 15+ stakeholders depend on daily for inventory, forecasting, and operational decisions.

**My Philosophy:** Choose the simplest architecture that solves the problem. Complexity should be a last resort, not a flex.

---

## Production Impact

| Metric | Before | After | Impact |
|--------|--------|-------|--------|
| **ETL Processing Time** | 4 hours | 30 minutes | **87% faster** |
| **Data Freshness** | 48 hours | 2-4 hours | **90% improvement** |
| **Pipeline Reliability** | ~70% | 95%+ | **25% more reliable** |
| **Analyst Time on Wrangling** | 60% | 15% | **45% time reclaimed** |
| **Data Quality Issues** | Unknown | 500+ caught | **Automated detection** |

---

## Featured Projects

### 🏗️ [Medallion Data Lakehouse](enterprise-data-platform/)
A Bronze/Silver/Gold architecture processing data from 4 source systems (ERP, CRM, WMS, OBI) into analytics-ready star schema models.

**Scale:** 10,000+ SKUs | 50+ pipelines | 15 fact tables | 6 dimensions

**Key Decision:** Chose Polars over Pandas after benchmarks showed 5-10x speedup on our data volumes. Chose DuckDB over PostgreSQL for analytics because zero-copy Parquet reads eliminated query latency.

[📖 Full Case Study →](enterprise-data-platform/)

---

### 🔍 [3-Tier Anomaly Detection System](enterprise-data-platform/data-quality/)
Multi-layer validation framework that caught 500+ data quality issues before they reached reports.

**Architecture:**
```
Tier 1: Schema Validation    → Block pipeline on failure
Tier 2: Business Rules       → Flag + Continue
Tier 3: Statistical Outliers → Alert + Log
```

**Result:** 70% reduction in data-related incidents after deployment.

---

### 🤖 [RPA Bot Framework](enterprise-data-platform/etl-framework/extractors/)
Selenium/PyAutoGUI bots for legacy systems without API access. Fragile by nature, but better than manual exports.

**Why RPA?** WMS had no API. Options were: (1) manual exports daily forever, (2) convince vendor to add API (6+ months), or (3) automate the clicks. I chose pragmatism over purity.

---

### 📈 [Demand Forecasting Engine](enterprise-data-platform/)
15+ algorithm ensemble with automatic pattern classification (ADI/CV² method) to select optimal forecasting approach per SKU.

**Algorithms:** ARIMA, Prophet, Exponential Smoothing, Croston's Method (intermittent demand), and more.

---

## Technical Stack

### Core Technologies
| Layer | Technology | Why This Choice |
|-------|------------|-----------------|
| **Processing** | Polars | 5-10x faster than Pandas, lazy evaluation, native Parquet support |
| **Analytics** | DuckDB | Zero-copy Parquet reads, embedded (no server), vectorized execution |
| **Storage** | Parquet | Columnar, compressed (80% smaller than CSV), schema evolution |
| **Automation** | Selenium | Only option for legacy systems without APIs |
| **Orchestration** | Task Scheduler | Enterprise constraint; would use Airflow in greenfield |

### Architecture Patterns
- **Medallion Lakehouse** (Bronze → Silver → Gold)
- **Star Schema** dimensional modeling (Kimball methodology)
- **Configuration-Driven ETL** (YAML configs, ABC patterns)
- **3-Tier Data Validation** (Schema → Business → Statistical)

### Languages & Tools
```
Python 3.10+     SQL (T-SQL, PL/SQL)     Git
Polars           DuckDB                   Docker (learning)
Pandas           SQL Server               GitHub Actions
Selenium         Oracle                   Power BI
PyAutoGUI        Parquet/Hive             Streamlit
```

---

## Architecture Decisions

I document major technical decisions. Here's a sample:

### Why Polars Over Pandas?

**Context:** Processing 10,000+ SKUs with 4-hour batch windows

**Decision:** Polars with lazy evaluation

**Rationale:**
- 5-10x faster on our data volume (benchmarked)
- 60% less memory via lazy evaluation
- Native multi-threading (no GIL)
- First-class Parquet support

**Trade-offs:**
- ❌ Spark: Overkill for single-machine workloads
- ❌ Pandas: Memory issues at 500MB+ DataFrames
- ✅ Polars: Sweet spot for our 2-5GB daily volume

**Migration Path:** If daily volume exceeds 50GB, migrate to Spark/Databricks.

[More ADRs →](enterprise-data-platform/docs/architecture-decisions/)

---

## Project Structure

```
data-engineering-portfolio/
├── README.md                    # You are here
├── enterprise-data-platform/    # Main showcase project
│   ├── architecture/            # System design docs
│   ├── etl-framework/           # Production ETL code
│   │   ├── extractors/          # DB, API, RPA extractors
│   │   ├── transformers/        # Cleaners, validators
│   │   └── loaders/             # Parquet, DuckDB writers
│   ├── data-quality/            # Anomaly detection system
│   └── docs/                    # Technical documentation
├── projects/                    # Case study HTML pages
└── index.html                   # Portfolio website
```

---

## What I'm Learning (2026)

| Technology | Status | Goal |
|------------|--------|------|
| **Microsoft Fabric** | 🟡 In Progress | DP-700 Certification Q1 |
| **Databricks** | 🟡 In Progress | Associate Cert Q2 |
| **Apache Spark** | 📖 Studying | Distributed processing |
| **dbt** | 📖 Studying | Transformation layer |

**Why these?** My current stack works for single-machine workloads. Cloud data platforms are the next scale tier.

---

## Contact

Building data infrastructure for supply chain operations.

- **Portfolio:** [godsonkurishinkal.github.io/data-engineering-portfolio](https://godsonkurishinkal.github.io/data-engineering-portfolio/)
- **LinkedIn:** [linkedin.com/in/godsonkurishinkal](https://linkedin.com/in/godsonkurishinkal)
- **Email:** godson.kurishinkal@gmail.com
- **GitHub:** [github.com/GodsonKurishinkal](https://github.com/GodsonKurishinkal)

---

## License

MIT License - See [LICENSE](LICENSE) for details