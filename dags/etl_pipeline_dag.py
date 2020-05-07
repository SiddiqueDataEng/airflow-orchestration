"""
ETL Pipeline DAG
Orchestrates daily ETL workflow with Airflow
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# Create DAG
dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for data warehouse',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'production']
)


def extract_from_source(**context):
    """Extract data from source systems"""
    logger.info("Extracting data from source...")
    execution_date = context['execution_date']
    logger.info(f"Execution date: {execution_date}")
    
    # Simulate extraction
    records_extracted = 10000
    context['task_instance'].xcom_push(key='records_extracted', value=records_extracted)
    
    logger.info(f"Extracted {records_extracted} records")
    return records_extracted


def transform_data(**context):
    """Transform extracted data"""
    logger.info("Transforming data...")
    
    records_extracted = context['task_instance'].xcom_pull(
        task_ids='extract_data',
        key='records_extracted'
    )
    
    logger.info(f"Processing {records_extracted} records")
    
    # Simulate transformation
    records_transformed = int(records_extracted * 0.95)  # 5% filtered out
    context['task_instance'].xcom_push(key='records_transformed', value=records_transformed)
    
    logger.info(f"Transformed {records_transformed} records")
    return records_transformed


def load_to_warehouse(**context):
    """Load data to warehouse"""
    logger.info("Loading data to warehouse...")
    
    records_transformed = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='records_transformed'
    )
    
    logger.info(f"Loading {records_transformed} records")
    
    # Simulate loading
    records_loaded = records_transformed
    context['task_instance'].xcom_push(key='records_loaded', value=records_loaded)
    
    logger.info(f"Loaded {records_loaded} records successfully")
    return records_loaded


def validate_data_quality(**context):
    """Validate data quality"""
    logger.info("Validating data quality...")
    
    records_loaded = context['task_instance'].xcom_pull(
        task_ids='load_data',
        key='records_loaded'
    )
    
    # Simulate quality checks
    quality_score = 0.98
    
    if quality_score < 0.95:
        raise ValueError(f"Data quality check failed: {quality_score}")
    
    logger.info(f"Data quality validation passed: {quality_score}")
    return quality_score


def send_success_notification(**context):
    """Send success notification"""
    logger.info("Sending success notification...")
    
    records_loaded = context['task_instance'].xcom_pull(
        task_ids='load_data',
        key='records_loaded'
    )
    
    message = f"ETL pipeline completed successfully. Records loaded: {records_loaded}"
    logger.info(message)
    return message


# Define tasks
start = DummyOperator(
    task_id='start',
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_source,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_warehouse,
    provide_context=True,
    dag=dag
)

validate_quality = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag
)

# Spark job for aggregations
run_aggregations = BashOperator(
    task_id='run_aggregations',
    bash_command='spark-submit /opt/airflow/jobs/aggregations.py ',
    dag=dag
)

# Update statistics
update_stats = BashOperator(
    task_id='update_statistics',
    bash_command='psql -h warehouse -U admin -d analytics -c "ANALYZE;"',
    dag=dag
)

send_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Define task dependencies
start >> extract_data >> transform_data_task >> load_data
load_data >> validate_quality >> [run_aggregations, update_stats]
[run_aggregations, update_stats] >> send_notification >> end
