# main_dag.py

# Import necessary libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

# Import other modules from your project
from extract_task import extract_task

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args={
        'owner': 'team',
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='@daily',
)

# Set task dependencies for the ETL pipeline
extract_task