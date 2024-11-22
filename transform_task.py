import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3

# daliy_weather
def transform_daily_and_wind_strength(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='cleaned_file_path')
    df = pd.read_csv(file_path)

    # Calculate daily averages
    daily_avg = df.groupby('Formatted Date').agg(
        avg_temperature_c=('Temperature (C)', 'mean'),
        avg_apparent_temperature_c=('Apparent Temperature (C)','mean'),
        avg_humidity=('Humidity', 'mean'),
        avg_wind_speed_kmh=('Wind Speed (km/h)', 'mean'),
        avg_visibility_km = ('Visibility (km)','mean'),
        avg_pressure_millibars=('Pressure (millibars)','mean'),
).reset_index()
    
    # Classify wind strength
    bins=[0,1.5,3.3,5.4,7.9,10.7,13.8,17.1,20.7,24.4,28.4,32.6,float('inf')]
    labels = ['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 
              'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 'Storm', 'Violent Storm']
    df['wind_strength'] = pd.cut(df['Wind Speed (km/h)'] / 3.6, bins=bins, labels=labels, right=True)
    
    # Prepare wind strength summary by day
    # Group by 'Formatted Date' and find the most common wind strength
    wind_strength_summary = df.groupby('Formatted Date')['wind_strength'].agg(lambda x: x.mode()[0]).reset_index()

    # Merge daily averages with wind strength
    daily_weather = pd.merge(daily_avg, wind_strength_summary, on='Formatted Date', how='left')

    # Rename column 'Formatted Date' to 'formatted_date'
    daily_weather = daily_weather.rename(columns={'Formatted Date': 'formatted_date'})

    transform_daily_weather_file_path = '/tmp/daily_avg_and_wind_strength_data.csv'
    # save file
    daily_weather.to_csv(transform_daily_weather_file_path,index = False) 
    # XCom push data
    kwargs['ti'].xcom_push(key='transform_daily_weather_file_path', value=transform_daily_weather_file_path)



# monthly_weather
# monthly_weather
def transform_monthly_and_mode_precip(**kwargs):
    cleaned_file_path = kwargs['ti'].xcom_pull(key='cleaned_file_path')
    df = pd.read_csv(cleaned_file_path)

    # Convert 'Formatted Date' to datetime and extract month names
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])
    df['month'] = df['Formatted Date'].dt.strftime('%B')  # Extract full month name (e.g., January)

    # Monthly Mode for Precipitation Type
    monthly_mode = df.groupby('month')['Precip Type'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA
    ).reset_index()
    monthly_mode.columns = ['month', 'mode_precip_type']

    # Calculate overall monthly averages across all years
    monthly_avg = df.groupby('month').agg(
        avg_temperature_c=('Temperature (C)', 'mean'),
        avg_apparent_temperature_c=('Apparent Temperature (C)', 'mean'),
        avg_humidity=('Humidity', 'mean'),
        avg_wind_speed_kmh=('Wind Speed (km/h)', 'mean'),
        avg_visibility_km=('Visibility (km)', 'mean'),
        avg_pressure_millibars=('Pressure (millibars)', 'mean')
    ).reset_index()

    # Merge average and mode data
    monthly_weather = pd.merge(monthly_avg, monthly_mode, on='month', how='left')

    # Ensure the months are ordered correctly (January to December)
    month_order = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    monthly_weather['month'] = pd.Categorical(monthly_weather['month'], categories=month_order, ordered=True)
    monthly_weather = monthly_weather.sort_values('month').reset_index(drop=True)


    transform_monthly_weather_file_path = '/tmp/monthly_avg_and_mode_precip_data.csv'
    monthly_weather.to_csv(transform_monthly_weather_file_path, index=False)
    
    # XCom for Transformation
    kwargs['ti'].xcom_push(key='transform_monthly_weather_file_path', value = transform_monthly_weather_file_path)
    
