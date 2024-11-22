# validate_task.py

# Import necessary libraries
import pandas as pd

# Function to validate data
def validate_data(**kwargs):
    # Pull the file paths from XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='transform_daily_weather_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='transform_monthly_weather_file_path')
    
    if not daily_file_path or not monthly_file_path:
        raise ValueError("File paths for daily or monthly data are missing")
    
    # Read the data into DataFrames
    daily_df = pd.read_csv(daily_file_path)
    monthly_df = pd.read_csv(monthly_file_path)
    
    # Check if there are any missing values
    if daily_df.isnull().sum().sum() > 0:
        print("Daily data contains missing values")
        print(daily_df.isnull().sum())
        raise ValueError("Daily data contains missing values")
    if monthly_df.isnull().sum().sum() > 0:
        print("Monthly data contains missing values")
        print(monthly_df.isnull().sum())
        raise ValueError("Monthly data contains missing values")
    
    # Verify that values fall within expected ranges for daily data
    assert daily_df['avg_temperature_c'].between(-50, 50).all(), "Temperature out of range in daily data"
    assert daily_df['avg_humidity'].between(0, 1).all(), "Humidity out of range in daily data"
    assert daily_df['avg_wind_speed_kmh'].ge(0).all(), "Wind Speed out of range in daily data"
    
    # Verify that values fall within expected ranges for monthly data
    assert monthly_df['avg_temperature_c'].between(-50, 50).all(), "Average temperature out of range in monthly data"
    assert monthly_df['avg_humidity'].between(0, 1).all(), "Average humidity out of range in monthly data"
    assert monthly_df['avg_wind_speed_kmh'].ge(0).all(), "Average wind speed out of range in monthly data"