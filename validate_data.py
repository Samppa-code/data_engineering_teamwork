import pandas as pd

def validate_data(**kwargs):
    # Load the data from the XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='transform_daily_weather_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='transform_monthly_weather_file_path')
    
    # Read the data into DataFrames
    daily_df = pd.read_csv(daily_file_path)
    monthly_df = pd.read_csv(monthly_file_path)

    # Check if there are any missing values
    assert daily_df.isnull().sum().sum() == 0, "Daily data contains missing values"
    assert monthly_df.isnull().sum().sum() == 0, "Monthly data contains missing values"

    # Verify that values fall within expected ranges for daily data
    assert daily_df['temperature_c'].between(-50, 50).all(), "Temperature out of range in daily data"
    assert daily_df['humidity'].between(0, 1).all(), "Humidity out of range in daily data"
    assert daily_df['wind_speed_kmh'].ge(0).all(), "Wind Speed out of range in daily data"

    # Verify that values fall within expected ranges for monthly data
    assert monthly_df['avg_temperature_c'].between(-50, 50).all(), "Average temperature out of range in monthly data"
    assert monthly_df['avg_humidity'].between(0, 1).all(), "Average humidity out of range in monthly data"
    assert monthly_df['avg_wind_speed_kmh'].ge(0).all(), "Average wind speed out of range in monthly data"

    