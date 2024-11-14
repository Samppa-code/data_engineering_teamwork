# main_dag.py

# Import necessary libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

# Import other modules from your project
from extract_task import extract_file

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
# TASK 1: Extract task to download and extract the dataset from Kaggle
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_file,
    dag=dag,
)

# TASK 2: Transform the dataset
# extracted_file_path = kwargs['ti'].xcom_pull(key='csv_file_path', task_ids='extract_task')


# TASK 3: Load the transformed dataset into a database


# Set task dependencies for the ETL pipeline
extract_task