<div align="center">

<!-- Animated Header -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=0:3776AB,100:FF6B6B&height=200&section=header&text=Godson%20Kurishinkal&fontSize=50&fontColor=ffffff&animation=fadeIn&fontAlignY=35&desc=Senior%20Data%20Engineer%20%7C%20Analytics%20Engineering%20%26%20BI&descSize=20&descAlignY=55" width="100%"/>

<br>

*Engineering data architecture to support analytics and business intelligence*

<br>

<!-- Social Badges -->
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/godsonkurishinkal)
[![Email](https://img.shields.io/badge/Email-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:godson.kurishinkal@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/GodsonKurishinkal)

<br>

<!-- Profile Views -->
![Profile Views](https://komarev.com/ghpvc/?username=GodsonKurishinkal&color=3776AB&style=flat-square&label=Profile+Views)

</div>

<br>

## 🎯 About Me

```python
class SeniorDataEngineer:
    def __init__(self):
        self.name = "Godson Kurishinkal"
        self.role = "Senior Data Engineer | Analytics Engineering & BI"
        self.location = "Dubai, UAE"
        self.company = "Landmark Group"
        
    def what_i_build(self):
        return """
        Analytics architecture and BI infrastructure—dimensional models,
        transformation layers, semantic consistency across 50+ reports.
        Turning operational data into automated business decisions.
        """
    
    def daily_stack(self):
        return ["Python", "SQL", "Polars", "DuckDB", "Azure", "Databricks", "Power BI"]
```

---

## 📊 Impact at a Glance

<div align="center">

<table>
<tr>
<td align="center" width="25%">
<img src="https://img.shields.io/badge/87%25-Faster-success?style=for-the-badge" alt="87%"/>
<br><b>Data Extraction</b>
<br><sub>4 hrs → 30 min</sub>
</td>
<td align="center" width="25%">
<img src="https://img.shields.io/badge/95%25+-Reliability-blue?style=for-the-badge" alt="95%+"/>
<br><b>Pipeline Uptime</b>
<br><sub>50+ ETL jobs</sub>
</td>
<td align="center" width="25%">
<img src="https://img.shields.io/badge/90%25-Fresher-orange?style=for-the-badge" alt="90%"/>
<br><b>Data Freshness</b>
<br><sub>48 hrs → 2-4 hrs</sub>
</td>
<td align="center" width="25%">
<img src="https://img.shields.io/badge/10K+-SKUs-red?style=for-the-badge" alt="10K+"/>
<br><b>Daily Processing</b>
<br><sub>Medallion Architecture</sub>
</td>
</tr>
</table>

</div>

---

## 🛠️ Tech Arsenal

<div align="center">

### Languages & Core
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![DAX](https://img.shields.io/badge/DAX-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

### Data Processing
![Polars](https://img.shields.io/badge/Polars-CD792C?style=for-the-badge&logo=polars&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

### Cloud & Infrastructure
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)

### Storage & Formats
![Parquet](https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apache&logoColor=white)
![ADLS](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)

### ML & Analytics
![Scikit-learn](https://img.shields.io/badge/Scikit--learn-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white)
![Prophet](https://img.shields.io/badge/Prophet-3B5998?style=for-the-badge&logo=meta&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-189FDD?style=for-the-badge&logo=xgboost&logoColor=white)

### Visualization & BI
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

### Automation
![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)
![PyAutoGUI](https://img.shields.io/badge/PyAutoGUI-3776AB?style=for-the-badge&logo=python&logoColor=white)

</div>

---

## 🏗️ Production Systems I've Built

<details>
<summary>🔷 <b>Medallion Data Lakehouse</b> — Enterprise Data Platform</summary>
<br>

Processing retail operations data through a three-layer architecture:

```mermaid
graph LR
    A[�� Sources] --> B[🥉 Bronze]
    B --> C[🥈 Silver]
    C --> D[🥇 Gold]
    
    subgraph Sources
    A1[ERP] --> A
    A2[CRM] --> A
    A3[WMS] --> A
    A4[OBI] --> A
    end
```

| Layer | Purpose | Tech |
|-------|---------|------|
| **Bronze** | Raw ingestion, schema-on-read | Polars, Parquet |
| **Silver** | Validated, deduplicated, typed | DuckDB, Hive Partitioning |
| **Gold** | Business aggregates, dimensions | Star Schema, Delta Lake |

</details>

<details>
<summary>📦 <b>Universal Replenishment Engine</b> — Inventory Optimization</summary>
<br>

Implementing sophisticated inventory policies:

- **(s,S) Policy** — Reorder point with order-up-to level
- **ABC-XYZ Classification** — Value × Demand variability matrix
- **Statistical Safety Stock** — Service level driven calculations
- **Scenario Configuration** — YAML-driven operational patterns

Supports: 3PL-to-warehouse, storage-to-picking, cross-dock operations

</details>

<details>
<summary>📈 <b>Demand Forecasting System</b> — 15+ Algorithm Ensemble</summary>
<br>

| Pattern Type | Classification | Algorithms |
|--------------|----------------|------------|
| Smooth | Low ADI, Low CV² | Moving Average, Exponential Smoothing |
| Intermittent | High ADI, Low CV² | Croston, SBA |
| Erratic | Low ADI, High CV² | ARIMA, Prophet |
| Lumpy | High ADI, High CV² | Ensemble with outlier handling |

Automated model selection based on ADI/CV² demand pattern classification.

</details>

<details>
<summary>🔍 <b>CBM Anomaly Detection</b> — Three-Tier Statistical System</summary>
<br>

```
Tier 1 ──► Validation Rules (Business Logic)
    │
    ▼
Tier 2 ──► Outlier Detection (IQR/Z-score)
    │
    ▼
Tier 3 ──► Volatility Analysis (Pattern Stability)
    │
    ▼
   📊 Priority Scoring ──► 500+ issues identified
```

</details>

<details>
<summary>🤖 <b>RPA Bot Framework</b> — Legacy System Automation</summary>
<br>

**Systems Integrated:** OBI, GCRM, GDMS, WMS

**Design Patterns:**
- Abstract Base Classes for bot behaviors
- Factory Pattern for bot instantiation
- Exponential Backoff for retry logic
- Structured logging for debugging

</details>

---

## ⚙️ Engineering Philosophy

<div align="center">

| Principle | Why It Matters |
|-----------|----------------|
| 🏛️ **Medallion Architecture** | Data quality gates at each layer |
| 🔄 **Idempotent Pipelines** | Safe re-runs, no duplicates |
| 📝 **Config-Driven Design** | YAML/dataclasses over hardcoding |
| 🛡️ **Error Handling** | Structured logging, graceful failures |
| 📐 **Type Hints** | Self-documenting, IDE-friendly code |
| 🧩 **Design Patterns** | Strategy, Factory, Template Method |

</div>

---

## 📍 Current Status

<div align="center">

| | |
|:---:|:---|
| 💼 | **Assistant Manager - MIS & Analytics** @ Landmark Group |
| 🎓 | **BS Data Science** @ IIT Madras *(in progress)* |
| 🎯 | **Focus:** Azure Databricks, Delta Lake, Microsoft Fabric |
| 📜 | **Pursuing:** DP-700, Databricks Data Engineer Associate & Professional |
| 🔍 | **Open to:** Senior/Lead/Staff Data Engineer roles in UAE |

</div>

---

## 📁 Featured Repositories

<div align="center">

| Repository | Description | Status |
|:-----------|:------------|:------:|
| `medallion-lakehouse` | Production lakehouse patterns with Polars + DuckDB | ✅ |
| `etl-framework` | Config-driven ETL with retry logic & logging | ✅ |
| `demand-forecasting` | ML ensemble for retail demand patterns | 🚧 |
| `rpa-automation` | Legacy system integration framework | 🚧 |

*Building public versions of production patterns*

</div>

---

## 📈 GitHub Stats

<div align="center">

<img src="https://github-readme-stats.vercel.app/api?username=GodsonKurishinkal&show_icons=true&theme=github_dark&hide_border=true&bg_color=0D1117&title_color=3776AB&icon_color=3776AB&text_color=FFFFFF" width="49%"/>
<img src="https://github-readme-streak-stats.herokuapp.com/?user=GodsonKurishinkal&theme=github-dark-blue&hide_border=true&background=0D1117&stroke=3776AB&ring=3776AB&fire=FF6B6B&currStreakLabel=FFFFFF" width="49%"/>

<br><br>

<img src="https://github-readme-stats.vercel.app/api/top-langs/?username=GodsonKurishinkal&layout=compact&theme=github_dark&hide_border=true&bg_color=0D1117&title_color=3776AB&text_color=FFFFFF" width="40%"/>

</div>

---

<div align="center">

<!-- Animated Footer -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=0:3776AB,100:FF6B6B&height=120&section=footer" width="100%"/>

**Dubai, UAE** · Open to Data Engineer opportunities

<sub>💡 *"Production code is code that someone depends on."*</sub>

</div>
