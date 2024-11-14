# extract_task.py

# Import necessary libraries
import os 
import zipfile 
from airflow.operators.python import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi
# Import dag from main_dag
from main_dag import dag

# Define the function to extract the file from Kaggle
def extract_file(**kwargs):
    # Create a Kaggle API instance
    api = KaggleApi()
    api.authenticate()
    
    # Download the file from Kaggle to the current directory
    api.dataset_download_file("muthuj7/weather-dataset", path=os.getenv('PROJECT_ROOT', './data'))
    
    # Define paths for the file
    downloaded_file_path = os.path.join('PROJECT_ROOT', './data/weather.csv')
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if zipfile.is_zipfile(zip_file_path):
        # Extract the file to the current directory if it's a ZIP file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('PROJECT_ROOT, ./data')
        # Remove the ZIP file
        os.remove(zip_file_path)
    
    # Push the file to XCom for further processing
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

# Define the task
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_file,
    dag=dag,
)