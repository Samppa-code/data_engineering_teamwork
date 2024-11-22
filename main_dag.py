# main_dag.py

# Import necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Import task functions
from data_engineering_teamwork.extract_task import extract_file
from data_engineering_teamwork.transform_task_clean import clean_data
from data_engineering_teamwork.transform_task import transform_daily_and_wind_strength, transform_monthly_and_mode_precip
from data_engineering_teamwork.validate_task import validate_data
from data_engineering_teamwork.load_task import load_data

# Default arguments for the DAG
default_args = {
    'owner': 'team2',
    'start_date': datetime(2024, 11, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'Teamwork_weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data teamwork',
    schedule_interval='@daily',
)

# Define tasks
# Task 1: Download and extract the dataset
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_file,
    provide_context=True,
    dag=dag,
)

# Task 2: Clean the dataset
clean_task = PythonOperator(
    task_id='clean_task',
    python_callable=clean_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 3a: Transform the dataset
transform_daily_and_wind_strength_task = PythonOperator(
    task_id='transform_daily_and_wind_strength_task',
    python_callable=transform_daily_and_wind_strength,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 3b: Transform the dataset
transform_monthly_and_mode_precip_task = PythonOperator(
    task_id='transform_monthly_and_mode_precip_task',
    python_callable=transform_monthly_and_mode_precip,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 4: Validate the transformed dataset
validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 5: Load the transformed dataset
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Define the task order
extract_task >> clean_task >> [transform_daily_and_wind_strength_task, transform_monthly_and_mode_precip_task] >> validate_task >> load_task