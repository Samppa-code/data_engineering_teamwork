from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi
import validate_data

# Default arguments for the DAG
default_args = {
    'owner': 'Team_2',
    'depends_on_past': False,
    'start_date': datetime(2024,11,14),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data with validation and trigger rules',
    schedule_interval='@daily',
)
# Task definitions

# Extract data task

# Transform data task

# Validate data task
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Load data task

# Set task dependencies
extract_task >> transform_task >> validate_task >> load_task
