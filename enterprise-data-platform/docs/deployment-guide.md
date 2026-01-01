# Deployment Guide

> Step-by-step guide for deploying the Enterprise Data Platform

## Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| Python | 3.10+ | 3.11+ |
| RAM | 8 GB | 16 GB |
| Storage | 50 GB | 200 GB (SSD) |
| OS | Windows 10/11, Linux | Windows Server 2019+ |

### Required Software

- Python 3.11+
- SQL Server ODBC Driver 17+
- Chrome/Chromium (for RPA extractors)
- ChromeDriver (matching Chrome version)
- Git

---

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/GodsonKurishinkal/data-engineering-portfolio.git
cd data-engineering-portfolio/enterprise-data-platform
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**requirements.txt:**
```text
polars>=0.20.0
duckdb>=0.9.0
pyodbc>=5.0.0
httpx>=0.25.0
selenium>=4.15.0
python-dotenv>=1.0.0
pydantic>=2.5.0
loguru>=0.7.0
tenacity>=8.2.0
pyarrow>=14.0.0
```

### 4. Install ODBC Drivers

**Windows:**
```powershell
# Download and install from Microsoft
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

**Linux (Ubuntu/Debian):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

### 5. Install ChromeDriver (for RPA)

```bash
# Check Chrome version
google-chrome --version

# Download matching ChromeDriver
# https://chromedriver.chromium.org/downloads

# Add to PATH
export PATH=$PATH:/path/to/chromedriver
```

---

## Configuration

### 1. Environment Variables

Create `.env` file in project root:

```env
# Database Connections
ERP_SERVER=erp-server.company.local
ERP_DATABASE=ERP_Production
ERP_USERNAME=etl_user
ERP_PASSWORD=secure_password

WMS_SERVER=wms-server.company.local
WMS_DATABASE=WMS_Production
WMS_USERNAME=etl_user
WMS_PASSWORD=secure_password

# API Credentials
CRM_API_BASE_URL=https://crm.company.com/api
CRM_API_KEY=your_api_key_here

# RPA Settings
WMS_WEB_URL=https://wms-legacy.company.local
WMS_WEB_USERNAME=rpa_user
WMS_WEB_PASSWORD=secure_password

# Storage Paths
BRONZE_PATH=/data/lakehouse/bronze
SILVER_PATH=/data/lakehouse/silver
GOLD_PATH=/data/lakehouse/gold

# DuckDB
DUCKDB_PATH=/data/analytics/warehouse.duckdb

# Logging
LOG_LEVEL=INFO
LOG_PATH=/var/log/data-platform
```

### 2. Create Data Directories

```bash
# Create lakehouse structure
mkdir -p /data/lakehouse/{bronze,silver,gold}
mkdir -p /data/analytics
mkdir -p /var/log/data-platform

# Set permissions
chmod -R 755 /data/lakehouse
chmod -R 755 /data/analytics
```

### 3. Initialize DuckDB

```python
import duckdb

conn = duckdb.connect('/data/analytics/warehouse.duckdb')

# Create schemas
conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

conn.close()
```

---

## Pipeline Configuration

### 1. Extraction Config

Create `config/extraction_config.yaml`:

```yaml
extractors:
  erp_inventory:
    type: database
    connection:
      server: ${ERP_SERVER}
      database: ${ERP_DATABASE}
      driver: "ODBC Driver 17 for SQL Server"
    query: |
      SELECT item_code, location_code, quantity_on_hand, 
             last_modified_date
      FROM inventory_master
      WHERE last_modified_date >= ?
    incremental_column: last_modified_date
    schedule: "0 6 * * *"  # Daily at 6 AM
    
  wms_movements:
    type: rpa
    url: ${WMS_WEB_URL}
    export_path: /tmp/wms_exports
    schedule: "0 7 * * *"  # Daily at 7 AM
    
  crm_customers:
    type: api
    base_url: ${CRM_API_BASE_URL}
    endpoint: /customers
    auth_type: api_key
    pagination: offset
    page_size: 1000
    schedule: "0 * * * *"  # Hourly
```

### 2. Transformation Config

Create `config/transformation_config.yaml`:

```yaml
transformations:
  bronze_to_silver:
    inventory:
      source: bronze/erp/inventory
      target: silver/inventory/stock_levels
      schema:
        item_code: string
        location_code: string
        quantity: int64
        snapshot_date: date
      quality_rules:
        - type: not_null
          columns: [item_code, location_code]
        - type: range
          column: quantity
          min: 0
          max: 1000000
          
  silver_to_gold:
    fact_inventory:
      sources:
        - silver/inventory/stock_levels
        - silver/product/master
        - silver/location/master
      target: gold/fact_inventory
      join_keys:
        - left: item_code
          right: product_code
```

---

## Running Pipelines

### Manual Execution

```python
from etl_framework.extractors import DatabaseExtractor
from etl_framework.transformers import DataCleaner, SchemaEnforcer
from etl_framework.loaders import ParquetWriter

# Initialize components
extractor = DatabaseExtractor({
    "connection_string": "...",
    "query": "SELECT * FROM inventory"
})

cleaner = DataCleaner({})
writer = ParquetWriter({
    "base_path": "/data/lakehouse/bronze/inventory",
    "partition_cols": ["snapshot_date"]
})

# Execute pipeline
raw_data = extractor.extract({})
clean_data = cleaner.transform(raw_data, {})
writer.load(clean_data, {})
```

### Scheduled Execution (Windows Task Scheduler)

```powershell
# Create scheduled task
$action = New-ScheduledTaskAction -Execute "python" -Argument "C:\data-platform\run_daily_pipeline.py"
$trigger = New-ScheduledTaskTrigger -Daily -At 6:00AM
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount

Register-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -TaskName "DailyETL"
```

### Scheduled Execution (Linux cron)

```bash
# Add to crontab
crontab -e

# Daily extraction at 6 AM
0 6 * * * cd /opt/data-platform && .venv/bin/python run_daily_pipeline.py >> /var/log/etl.log 2>&1

# Hourly CRM sync
0 * * * * cd /opt/data-platform && .venv/bin/python run_crm_sync.py >> /var/log/etl.log 2>&1
```

---

## Monitoring Setup

### 1. Logging Configuration

```python
# logging_config.py
from loguru import logger
import sys

logger.remove()  # Remove default handler

# Console output
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> | {message}",
    level="INFO"
)

# File output with rotation
logger.add(
    "/var/log/data-platform/etl_{time:YYYY-MM-DD}.log",
    rotation="00:00",  # New file daily
    retention="30 days",
    compression="gz",
    level="DEBUG"
)

# Error file
logger.add(
    "/var/log/data-platform/errors.log",
    rotation="10 MB",
    level="ERROR"
)
```

### 2. Alerting Integration

```python
# alerts.py
import httpx

def send_teams_alert(title: str, message: str, severity: str = "high"):
    """Send alert to Microsoft Teams webhook."""
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    
    color = {
        "critical": "FF0000",
        "high": "FFA500", 
        "medium": "FFFF00",
        "low": "00FF00"
    }.get(severity, "808080")
    
    payload = {
        "@type": "MessageCard",
        "themeColor": color,
        "title": f"[{severity.upper()}] {title}",
        "text": message
    }
    
    httpx.post(webhook_url, json=payload)
```

---

## Health Checks

### Pipeline Health Check Script

```python
# health_check.py
import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

def check_data_freshness(layer_path: str, max_age_hours: int = 4) -> dict:
    """Check if data is fresh enough."""
    path = Path(layer_path)
    latest_file = max(path.rglob("*.parquet"), key=lambda p: p.stat().st_mtime, default=None)
    
    if not latest_file:
        return {"status": "error", "message": "No data files found"}
    
    age_hours = (datetime.now() - datetime.fromtimestamp(latest_file.stat().st_mtime)).total_seconds() / 3600
    
    return {
        "status": "ok" if age_hours <= max_age_hours else "stale",
        "latest_file": str(latest_file),
        "age_hours": round(age_hours, 2)
    }

def check_row_counts(duckdb_path: str) -> dict:
    """Check row counts in gold tables."""
    conn = duckdb.connect(duckdb_path, read_only=True)
    
    tables = ["fact_inventory", "fact_sales", "dim_product", "dim_location"]
    counts = {}
    
    for table in tables:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM gold.{table}").fetchone()
            counts[table] = result[0]
        except Exception as e:
            counts[table] = f"Error: {e}"
    
    conn.close()
    return counts

if __name__ == "__main__":
    print("=== Data Platform Health Check ===")
    print("\nData Freshness:")
    for layer in ["bronze", "silver", "gold"]:
        result = check_data_freshness(f"/data/lakehouse/{layer}")
        print(f"  {layer}: {result['status']} (age: {result.get('age_hours', 'N/A')} hours)")
    
    print("\nRow Counts:")
    counts = check_row_counts("/data/analytics/warehouse.duckdb")
    for table, count in counts.items():
        print(f"  {table}: {count:,}" if isinstance(count, int) else f"  {table}: {count}")
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| ODBC Connection Failed | `[08001] Unable to connect` | Check server name, firewall, driver version |
| ChromeDriver Mismatch | `SessionNotCreatedException` | Update ChromeDriver to match Chrome version |
| Permission Denied | `PermissionError` on write | Check directory permissions, run as correct user |
| Memory Error | `MemoryError` during transform | Use streaming/chunked processing, increase RAM |
| Stale Data | Data not updating | Check scheduler, source system availability |

### Debug Mode

```bash
# Run with debug logging
LOG_LEVEL=DEBUG python run_pipeline.py

# Check specific extractor
python -c "
from etl_framework.extractors import DatabaseExtractor
ext = DatabaseExtractor({...})
ext.validate_connection()  # Test connectivity
"
```

### Log Analysis

```bash
# Find errors in last 24 hours
grep -i error /var/log/data-platform/etl_*.log | tail -50

# Check pipeline duration
grep "Pipeline completed" /var/log/data-platform/etl_*.log | tail -10
```

---

## Backup & Recovery

### Data Backup

```bash
#!/bin/bash
# backup_data.sh

BACKUP_DATE=$(date +%Y%m%d)
BACKUP_PATH=/backups/data-platform/$BACKUP_DATE

# Backup DuckDB
mkdir -p $BACKUP_PATH
cp /data/analytics/warehouse.duckdb $BACKUP_PATH/

# Backup configs
cp -r /opt/data-platform/config $BACKUP_PATH/

# Compress
tar -czf $BACKUP_PATH.tar.gz $BACKUP_PATH
rm -rf $BACKUP_PATH

# Keep last 7 days
find /backups/data-platform -name "*.tar.gz" -mtime +7 -delete
```

### Recovery Procedure

1. **Stop all scheduled tasks**
2. **Restore from backup:**
   ```bash
   tar -xzf /backups/data-platform/20251231.tar.gz -C /tmp
   cp /tmp/20251231/warehouse.duckdb /data/analytics/
   ```
3. **Re-run failed pipelines from checkpoint**
4. **Verify data integrity**
5. **Resume scheduled tasks**

---

## Security Considerations

- Store credentials in `.env` file (never commit to git)
- Use service accounts with minimum required permissions
- Enable encryption at rest for sensitive data
- Rotate API keys and passwords regularly
- Audit access to data directories
- Use VPN/private network for database connections

---

*Last Updated: January 2026*
