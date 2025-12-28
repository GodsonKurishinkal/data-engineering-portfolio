<div align="center">

# Godson Kurishinkal

**Senior Data Engineer** · Dubai, UAE

*Building enterprise data platforms that power supply chain operations at scale*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/godsonkurishinkal)
[![Email](https://img.shields.io/badge/Email-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:godson.kurishinkal@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/GodsonKurishinkal)

<br>

![Python](https://img.shields.io/badge/Python-Expert-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Advanced-4479A1?style=flat-square&logo=postgresql&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)

</div>

---

## 🎯 What I Build

I design and build **production data infrastructure** for retail supply chain operations—not prototypes or dashboards, but the systems that **10,000+ SKUs depend on daily**.

My work sits at the intersection of **data engineering**, **applied ML**, and **supply chain operations**: building pipelines that ingest from legacy ERPs, transforming messy operational data into reliable analytics, and deploying ML models that actually run in production.

---

## 📊 Impact

<table>
<tr>
<td align="center"><h3>87%</h3><sub>Faster Data Extraction</sub><br><code>4 hrs → 30 min</code></td>
<td align="center"><h3>95%+</h3><sub>Pipeline Reliability</sub><br><code>50+ ETL jobs</code></td>
<td align="center"><h3>90%</h3><sub>Data Freshness</sub><br><code>48 hrs → 2-4 hrs</code></td>
<td align="center"><h3>10K+</h3><sub>SKUs Processed</sub><br><code>Medallion Architecture</code></td>
</tr>
</table>

---

## 🛠️ Technical Focus

| Domain | Technologies & Patterns |
|--------|------------------------|
| **Data Platforms** | Medallion lakehouse (Bronze/Silver/Gold), Star schema, Incremental processing |
| **Processing** | Python, SQL, Polars, DuckDB, PySpark |
| **Orchestration** | Production ETL frameworks, Retry logic, YAML-driven config |
| **ML Systems** | Demand forecasting (15+ ensembles), ABC-XYZ classification, Anomaly detection |
| **Cloud** | Azure Data Factory, Databricks, Synapse, Delta Lake, ADLS Gen2 |

---

## 🏗️ Production Systems

<details>
<summary><b>Medallion Data Lakehouse</b> — Enterprise data platform</summary>
<br>
Processing retail operations data through Bronze (raw ingestion), Silver (cleaned, validated), and Gold (business-ready) layers. Built with Polars + DuckDB for performance, Parquet + Hive partitioning for storage efficiency.

```
Bronze → Raw ingestion from ERP, CRM, WMS, OBI
Silver → Validated, deduplicated, type-enforced
Gold   → Business-ready aggregates & dimensions
```
</details>

<details>
<summary><b>Universal Replenishment Engine</b> — Inventory optimization</summary>
<br>
Implementing (s,S) policy with ABC-XYZ classification, statistical safety stock calculations, and scenario-based YAML configuration for different operational patterns (3PL-to-warehouse, storage-to-picking).
</details>

<details>
<summary><b>Demand Forecasting System</b> — 15+ algorithm ensemble</summary>
<br>
ADI/CV² demand pattern classification with automated model selection. Handles intermittent demand patterns common in retail—lumpy, erratic, smooth, and slow-moving SKUs.
</details>

<details>
<summary><b>CBM Anomaly Detection</b> — Three-tier statistical system</summary>
<br>

| Tier | Method | Purpose |
|------|--------|---------|
| 1 | Validation Rules | Business logic checks |
| 2 | IQR/Z-score | Outlier detection |
| 3 | Volatility Analysis | Pattern stability |

Identified 500+ data quality issues with priority scoring.
</details>

<details>
<summary><b>RPA Bot Framework</b> — Legacy system automation</summary>
<br>
Selenium/PyAutoGUI automation for OBI, GCRM, GDMS, WMS integration. Abstract base classes, factory pattern, exponential backoff retry logic.
</details>

---

## 💻 Tech Stack

```
┌─────────────────┬────────────────────────────────────────────────┐
│ Languages       │ Python (expert) · SQL · DAX                    │
├─────────────────┼────────────────────────────────────────────────┤
│ Processing      │ Polars · DuckDB · PySpark · Pandas             │
├─────────────────┼────────────────────────────────────────────────┤
│ Storage         │ Parquet · Delta Lake · Hive Partitioning       │
├─────────────────┼────────────────────────────────────────────────┤
│ Cloud           │ Azure (ADF, Synapse, ADLS Gen2) · Databricks   │
├─────────────────┼────────────────────────────────────────────────┤
│ Orchestration   │ Custom Python frameworks · Airflow patterns    │
├─────────────────┼────────────────────────────────────────────────┤
│ ML/Stats        │ Scikit-learn · Prophet · XGBoost · statsmodels │
├─────────────────┼────────────────────────────────────────────────┤
│ Visualization   │ Power BI · Streamlit                           │
├─────────────────┼────────────────────────────────────────────────┤
│ Automation      │ Selenium · PyAutoGUI                           │
└─────────────────┴────────────────────────────────────────────────┘
```

---

## ⚙️ Engineering Principles

```python
class ProductionCode:
    """The code I write follows patterns that matter in production."""
    
    patterns = [
        "Medallion architecture for data quality at each layer",
        "Idempotent pipelines that can be safely re-run",
        "Configuration-driven design (YAML, dataclasses) over hardcoding",
        "Comprehensive error handling with structured logging",
        "Type hints on all public interfaces",
        "Design patterns (Strategy, Factory, Template) where they reduce complexity",
    ]
```

---

## 📍 Currently

| | |
|---|---|
| **Role** | Senior Data Engineer @ Landmark Group |
| **Education** | BS Data Science & Applications, IIT Madras *(in progress)* |
| **Focus** | Cloud platform architecture (Azure/Databricks), MLOps |
| **Open to** | Senior Data Engineer roles in UAE |

---

## 📁 Featured Repositories

| Repository | Description | Status |
|------------|-------------|--------|
| `medallion-lakehouse` | Production lakehouse patterns | 🚧 Coming Soon |
| `etl-framework` | Configuration-driven ETL pipelines | 🚧 Coming Soon |
| `demand-forecasting` | ML ensemble for retail forecasting | 🚧 Coming Soon |

---

<div align="center">

**Dubai, UAE** · Open to Senior Data Engineer opportunities

[![GitHub Stats](https://github-readme-stats.vercel.app/api?username=GodsonKurishinkal&show_icons=true&theme=default&hide_border=true&hide=stars&count_private=true)](https://github.com/GodsonKurishinkal)

</div>
