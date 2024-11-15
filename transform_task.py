import pandas as pd

def transform_daily_and_wind_strength(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='cleaned_file_path')
    df = pd.read_csv(file_path)

    # Calculate daily averages for temperature, humidity, and wind speed
    daily_avg = df.groupby('Formatted Date').agg(
        avg_temperature_c=('Temperature (C)', 'mean'),
        avg_humidity=('Humidity', 'mean'),
        avg_wind_speed_kmh=('Wind Speed (km/h)', 'mean'),
    ).reset_index()
    
    # Classify wind strength
    bins = [0, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float('inf')]
    labels = ['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 
              'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 'Storm', 'Violent Storm']
    df['wind_strength'] = pd.cut(df['Wind Speed (km/h)'] / 3.6, bins=bins, labels=labels, right=True)

    # Handle missing values in wind_strength column by replacing them with the most frequent category
    most_frequent_category = df['wind_strength'].mode()[0]
    df['wind_strength'].fillna(most_frequent_category, inplace=True)

    # Keep only relevant columns and drop duplicates
    required_columns = df[['Formatted Date', 'Temperature (C)', 'Apparent Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)', 'wind_strength']].drop_duplicates()

    # Merge the daily averages with the other columns to be 'daily_weather'
    daily_weather = pd.merge(required_columns, daily_avg, on='Formatted Date', how='left')

    # Rename columns to match the 'daily_weather' table schema
    daily_weather.columns = [
        'formatted_date',     
        'temperature_c',      
        'apparent_temperature_c',  
        'humidity',          
        'wind_speed_kmh',     
        'visibility_km',      
        'pressure_millibars', 
        'wind_strength',     
        'avg_temperature',  
        'avg_humidity',     
        'avg_wind_speed_kmh'  
    ]

    transform_daily_weather_file_path = '/tmp/daily_avg_and_wind_strength_data.csv'
    daily_weather.to_csv(transform_daily_weather_file_path, index=False)
    kwargs['ti'].xcom_push(key='transform_daily_weather_file_path', value=transform_daily_weather_file_path)

def transform_monthly_and_mode_precip(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='cleaned_file_path')
    df = pd.read_csv(file_path)

    # Monthly Mode for Precipitation Type
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)
    df['month'] = df['Formatted Date'].dt.to_period('M')

    monthly_mode = df.groupby('month')['Precip Type'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA
    ).reset_index()
    monthly_mode.columns = ['month', 'mode_precip_type']
    
    monthly_avg = df.groupby('month').agg(
        avg_temperature_c=('Temperature (C)', 'mean'),
        avg_apparent_temperature_c=('Apparent Temperature (C)', 'mean'),
        avg_humidity=('Humidity', 'mean'),
        avg_wind_speed_kmh=('Wind Speed (km/h)', 'mean'),
        avg_visibility_km=('Visibility (km)', 'mean'),
        avg_pressure_millibars=('Pressure (millibars)', 'mean')
    ).reset_index()

    required_columns = monthly_avg[['month', 'avg_temperature_c', 'avg_apparent_temperature_c', 'avg_humidity', 'avg_wind_speed_kmh', 'avg_visibility_km', 'avg_pressure_millibars']]
    monthly_weather = pd.merge(required_columns, monthly_mode, on='month', how='left')

    transform_monthly_weather_file_path = '/tmp/monthly_avg_and_mode_precip_data.csv'
    monthly_weather.to_csv(transform_monthly_weather_file_path, index=False)
    kwargs['ti'].xcom_push(key='transform_monthly_weather_file_path', value=transform_monthly_weather_file_path)