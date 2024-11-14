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

# Define tasks

# Test start task
def print_start_message():
    print('Starting the ETL pipeline...')

start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_start_message,
    dag=dag,
)

# Test end task
def print_result():
    print('ETL pipeline completed successfully!')

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_result,
    dag=dag,
)


# Set task dependencies for the ETL pipeline
start_task >> end_task