# Godson Kurishinkal

**Senior Data Engineer** · Dubai, UAE

Building enterprise data platforms that power supply chain operations at scale.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/godsonkurishinkal)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat&logo=gmail&logoColor=white)](mailto:godson.kurishinkal@gmail.com)

---

## What I Build

I design and build **production data infrastructure** for retail supply chain operations—not prototypes or dashboards, but the systems that **10,000+ SKUs depend on daily**.

My work sits at the intersection of **data engineering**, **applied ML**, and **supply chain operations**: building pipelines that ingest from legacy ERPs, transforming messy operational data into reliable analytics, and deploying ML models that actually run in production.

---

## Impact

| | |
|---|---|
| **87%** | Reduction in data extraction time (4 hrs → 30 min) |
| **95%+** | Pipeline reliability across 50+ ETL jobs |
| **90%** | Improvement in data freshness (48 hrs → 2-4 hrs) |
| **10,000+** | SKUs processed through medallion architecture |

---

## Technical Focus

**Data Platforms** · Medallion lakehouse architecture (Bronze/Silver/Gold), dimensional modeling with star schema, incremental processing patterns

**Processing** · Python, SQL, Polars, DuckDB, PySpark — choosing the right tool for the scale

**Orchestration** · Production ETL frameworks with retry logic, error handling, YAML-driven configuration

**ML Systems** · Demand forecasting (15+ algorithm ensembles), inventory optimization (ABC-XYZ, safety stock), anomaly detection

**Cloud** · Azure Data Factory, Databricks, Synapse Analytics, Delta Lake

---

## Production Systems

### Medallion Data Lakehouse
Enterprise data platform processing retail operations data through Bronze (raw ingestion), Silver (cleaned, validated), and Gold (business-ready) layers. Built with Polars + DuckDB for performance, Parquet + Hive partitioning for storage efficiency.

### Universal Replenishment Engine
Inventory optimization system implementing (s,S) policy with ABC-XYZ classification, statistical safety stock calculations, and scenario-based YAML configuration for different operational patterns (3PL-to-warehouse, storage-to-picking).

### Demand Forecasting System
Ensemble forecasting with 15+ algorithms, ADI/CV² demand pattern classification, and automated model selection. Handles intermittent demand patterns common in retail.

### CBM Anomaly Detection
Three-tier statistical system: Tier 1 validation rules, Tier 2 outlier detection (IQR/Z-score), Tier 3 volatility analysis with priority scoring. Identified 500+ data quality issues.

### RPA Bot Framework
Selenium/PyAutoGUI automation for legacy system integration (OBI, GCRM, GDMS, WMS). Abstract base classes, factory pattern, exponential backoff retry logic.

---

## Tech Stack

\`\`\`
Languages        Python (expert) · SQL · DAX
Processing       Polars · DuckDB · PySpark · Pandas
Storage          Parquet · Delta Lake · Hive Partitioning
Cloud            Azure (Data Factory, Synapse, ADLS Gen2) · Databricks
Orchestration    Custom Python frameworks · Airflow patterns
ML/Stats         Scikit-learn · Prophet · XGBoost · statsmodels
Visualization    Power BI · Streamlit
Automation       Selenium · PyAutoGUI
\`\`\`

---

## Engineering Principles

The code I write follows patterns that matter in production:

- **Medallion architecture** for data quality at each layer
- **Idempotent pipelines** that can be safely re-run
- **Configuration-driven design** (YAML, dataclasses) over hardcoding
- **Comprehensive error handling** with structured logging
- **Type hints** on all public interfaces
- **Design patterns** (Strategy, Factory, Template Method) where they reduce complexity

---

## Currently

- **Role**: Senior Data Engineer at Landmark Group
- **Education**: BS Data Science & Applications, IIT Madras (in progress)
- **Focus**: Cloud platform architecture (Azure/Databricks), MLOps fundamentals
- **Looking for**: Senior Data Engineer roles in UAE

---

## Featured Repositories

*Building public versions of production patterns—coming soon.*

---

<sub>Based in Dubai, UAE · Open to Senior Data Engineer opportunities</sub>
