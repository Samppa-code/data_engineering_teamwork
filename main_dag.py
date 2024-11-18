# main_dag.py

# Import necessary libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

# Import other modules from the project
from extract_task import extract_file
from transform_task_clean import clean_data

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args={
        'owner': 'team',
        'start_date': datetime(2024, 11, 12),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule='@daily',
)

# DEFINE TASKS
# TASK 1: Download and extract the dataset from Kaggle
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_file,
    dag=dag,
)

# TASK 2a: Clean the extracted dataset
clean_task = PythonOperator(
    task_id='clean_task',
    python_callable=clean_data,
    dag=dag,
)
# TASK 2b:

# TASK 3:

# TASK 4:


# Set task dependencies for the ETL pipeline
extract_task >> clean_task