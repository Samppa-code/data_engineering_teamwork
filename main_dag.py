# main_dag.py

# Import necessary libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator

# Import other modules from your project
from extract_task import extract_file

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

# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_file,
    dag=dag,
)

# Set task dependencies for the ETL pipeline
extract_task