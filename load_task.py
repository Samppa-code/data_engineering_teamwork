import pandas as pd
import sqlite3

def load_data(**kwargs):
    # Pull the file paths from XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='transform_daily_weather_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='transform_monthly_weather_file_path')

    # Read the CSV files into DataFrames
    daily_df = pd.read_csv(daily_file_path)
    monthly_df = pd.read_csv(monthly_file_path)

    # Connect to the SQLite database
    # REMEMBER TO CHAGE THE PATH TO THE DATABASE to match the path in your environment
    conn = sqlite3.connect('/home/samu/airflow/databases/weather_data.db')
    cursor = conn.cursor()

    # Create the daily_weather table if it does not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formatted_date TEXT,
            temperature_c REAL,
            apparent_temperature_c REAL,
            humidity REAL,
            wind_speed_kmh REAL,
            visibility_km REAL,
            pressure_millibars REAL,
            wind_strength TEXT,
            avg_temperature REAL,
            avg_humidity REAL,
            avg_wind_speed_kmh REAL
        )
    ''')

    # Create the monthly_weather table if it does not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            avg_temperature_c REAL,
            avg_apparent_temperature_c REAL,
            avg_humidity REAL,
            avg_visibility_km REAL,
            avg_pressure_millibars REAL,
            mode_precip_type TEXT
        )
    ''')

    # Insert data into daily_weather table
    # statement uses placeholders (?) for the values to be inserted, 
    # which are provided as a tuple in the second argument of the execute() method.
    for index, row in daily_df.iterrows():
        cursor.execute('''
            INSERT INTO daily_weather (
                formatted_date, temperature_c, apparent_temperature_c, humidity, wind_speed_kmh, visibility_km, pressure_millibars, wind_strength, avg_temperature, avg_humidity, avg_wind_speed_kmh
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['formatted_date'], row['temperature_c'], row['apparent_temperature_c'], row['humidity'], row['wind_speed_kmh'], row['visibility_km'], row['pressure_millibars'], row['wind_strength'], row['avg_temperature'], row['avg_humidity'], row['avg_wind_speed_kmh']
        ))

    # Insert data into monthly_weather table
    # statement uses placeholders (?) for the values to be inserted,
    # which are provided as a tuple in the second argument of the execute() method
    for index, row in monthly_df.iterrows():
        cursor.execute('''
            INSERT INTO monthly_weather (
                month, avg_temperature_c, avg_apparent_temperature_c, avg_humidity, avg_visibility_km, avg_pressure_millibars, mode_precip_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['month'], row['avg_temperature_c'], row['avg_apparent_temperature_c'], row['avg_humidity'], row['avg_visibility_km'], row['avg_pressure_millibars'], row['mode_precip_type']
        ))

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()