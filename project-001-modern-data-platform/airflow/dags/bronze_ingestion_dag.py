"""
Airflow DAG: Bronze Layer Ingestion
====================================

Daily ingestion of M5 Walmart data into bronze layer.

Schedule: Daily @ 2:00 AM UTC
SLA: 30 minutes
Owner: Data Engineering Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
from pathlib import Path

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from ingestion.m5_ingestion import M5DataIngestion


# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30)
}

# DAG definition
dag = DAG(
    dag_id='bronze_ingestion',
    default_args=default_args,
    description='Daily M5 data ingestion to bronze layer',
    schedule_interval='0 2 * * *',  # 2:00 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'data-engineering']
)


def ingest_to_bronze(**context):
    """
    Ingest M5 data to bronze layer.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Starting bronze layer ingestion...")
    
    # Configuration
    SOURCE_PATH = '/path/to/m5/data'  # Update with actual path
    BRONZE_PATH = '/path/to/bronze'   # Update with actual path
    
    # Initialize ingestion
    ingestion = M5DataIngestion(
        source_path=SOURCE_PATH,
        bronze_path=BRONZE_PATH
    )
    
    # Run ingestion
    stats = ingestion.ingest_sales_data(validation_mode=True)
    
    # Push stats to XCom
    context['task_instance'].xcom_push(key='ingestion_stats', value=stats)
    
    logger.info(f"Bronze ingestion completed: {stats['total_records']:,} records")
    
    return stats


def validate_bronze(**context):
    """
    Validate bronze layer ingestion.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Pull stats from previous task
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_bronze',
        key='ingestion_stats'
    )
    
    # Validation checks
    assert stats['total_records'] > 0, "No records ingested"
    assert stats['stores'] > 0, "No stores found"
    assert stats['items'] > 0, "No items found"
    
    logger.info("✅ Bronze layer validation passed")
    
    return True


def send_notification(**context):
    """
    Send completion notification.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_bronze',
        key='ingestion_stats'
    )
    
    message = f"""
    Bronze Layer Ingestion Completed
    ================================
    
    Records: {stats['total_records']:,}
    Stores: {stats['stores']}
    Items: {stats['items']}
    Dates: {stats['dates']}
    
    Status: SUCCESS
    """
    
    logger.info(message)
    # In production: send to Slack, email, etc.
    
    return message


# Task definitions
t1_ingest = PythonOperator(
    task_id='ingest_bronze',
    python_callable=ingest_to_bronze,
    provide_context=True,
    dag=dag
)

t2_validate = PythonOperator(
    task_id='validate_bronze',
    python_callable=validate_bronze,
    provide_context=True,
    dag=dag
)

t3_notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    provide_context=True,
    dag=dag
)

# Task dependencies
t1_ingest >> t2_validate >> t3_notify
