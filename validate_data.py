import pandas as pd
def validate_data(**kwargs):
    # Load the data from the XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='transformed_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='monthly_file_path')
    # Read the data into a DataFrame
    daily_df = pd.read_csv(daily_file_path)
    monthly_df = pd.read_csv(monthly_file_path)

    # Check if there are any missing values
    assert daily_df.isnull().sum().sum() == 0, "Daily data contains missing values"
    assert monthly_df.isnull().sum().sum() == 0, "Monthly data contains missing values"

    # Verify that values fall withing expected ranges
    assert daily_df['Temperature (C)'].between(-50, 50).all(), "Temperature out of range in daily data"
    assert daily_df['Humidity'].between(0, 1).all(), "Humidity out of range in daily data"
    assert daily_df['Wind Speed (km/h)'].ge(0).all(), "Wind Speed out of range in daily data"