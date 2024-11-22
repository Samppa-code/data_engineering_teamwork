# cleaning_data.py

# Import necessary libraries
import pandas as pd

# Function to clean data
def clean_data(**kwargs):
    # Pull data from XCom
    extracted_file_path = kwargs['ti'].xcom_pull(key='csv_file_path', task_ids='extract_task')
    # If data is not found in XCom, raise an error
    if extracted_file_path is None:
        raise ValueError('No data found in XCom for cleaning task')

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(extracted_file_path)

    # Convert the 'Formatted Date' column to proper date format
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)
    # something                                      
    df['Formatted Date'] = df['Formatted Date'].dt.tz_convert('UTC+01:00')
    # (yyyy-mm-dd)
    df['Formatted Date'] = df['Formatted Date'].dt.tz_localize(None).dt.strftime('%Y-%m-%d')

    # Handle missing values in critical columns
    # Replace missing values in 'Temperature (C)' column with median
    df['Temperature (C)'].fillna(df['Temperature (C)'].median(), inplace=True)

    # Replace missing values in 'Humidity' column with median
    df['Humidity'].fillna(df['Humidity'].median(), inplace=True)

    # Replace missing values in 'Wind Speed (km/h)' column with median
    df['Wind Speed (km/h)'].fillna(df['Wind Speed (km/h)'].median(), inplace=True)

    # Check for duplicate rows and remove them
    df.drop_duplicates(inplace=True)

    # Save the cleaned DataFrame to a new CSV file
    cleaned_file_path = '/home/samu/airflow/datasets/weatherHistory_cleaned.csv'
    df.to_csv(cleaned_file_path, index=False)

    # Push the cleaned file path to XCom
    kwargs['ti'].xcom_push(key='cleaned_file_path', value=cleaned_file_path)