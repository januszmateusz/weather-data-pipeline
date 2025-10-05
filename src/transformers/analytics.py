from operator import index
import stat
from turtle import st
from numpy.ma import anom
import pandas as pd
from typing import Dict

def calculate_city_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate statistics grouped by city

    Args:
        def: Weather DataFrame with multiple timestamps

    Returns:
        pdDataFrame: City-level statistics
    """

    stats = df.groupby('city').agg({
        'temperature':['mean', 'min', 'max', 'std'],
        'humidity':['mean', 'max'],
        'wind_speed': 'mean',
        'timestamp':'count'
    }).round(2)

    #Flatten column names
    stats.columns=[' '.join(col).strip() for col in stats.columns.values]
    stats = stats.rename(columns={'timestamp_count': 'num_readings'})

    return stats.reset_index()

def calculate_country_statistics(df: pd.DataFrame) -> pd.DataFrame:
    stats = df.groupby('country').agg({
        'temperature': 'mean',
        'humidity': 'mean',
        'city': 'count'
    }).round(2)

    stats = stats.rename(columns={'city':'num_cities'})
    return stats.reset_index()

def analyze_temperature_trends(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """
    Analyze temperature trends for a specific city.

    Args:
        df: Wheater DataFrame with timestamp
        city: City name to analyze

    Returns:
        pd.DataFrmae: Hourly temperature trends
    """

    #Filter for city
    city_df  = df[df['city'] == city].copy

    #Convert timestamp to datetime
    city_df['timestamp'] = pd.to_datetime(city_df['timestamp'])

    #Set timestamp as index
    city_df=city_df.set_index('timestamp')

    #Sort by time:
    city_df = city_df.sort_index()

    #Resample to hourly average(ifmultiple readings per hour)
    hourly = city_df['temperature'].resample('H').mean()

    #Calculate rolling 3-hour average
    hourly_df = pd.DataFrame({
        'temperature': hourly,
        'temp_3h_avg': hourly.rolling(window=3).mean(),
        'temp_change': hourly.diff()
    })

    return hourly_df

def detect_temperature_anomalies(
    df: pd.DataFrame,
    threshold_std: float = 2.0
) -> pd.DataFrame:
    """
    Detect temperature anomalies using standard deviation.

    Args:
        df: Weather DataFrame
        threshold_std: Number of standard deviations for anomaly

    Returns:
        pd.DataFrame: Records with anomalous temperatures
    """

    mean_temp = df['temperature'].mean()
    std_temp = df['temperature'].std()

    anomalies = df[abs(df['temperature'] - mean_temp) > (threshold_std * std_temp)].copy()
    anomalies['deviation'] = ((anomalies['temperature'] - mean_temp) / std_temp).round(2)

    return anomalies

#Test with historical data:
if __name__ == "__main__":
    df = pd.read_csv("weather_data.csv")

    print("City Statistics:")
    print(calculate_city_statistics(df))
    print("\nCountry Statistics:")
    print(calculate_country_statistics(df))