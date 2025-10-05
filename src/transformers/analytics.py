import stat
from turtle import st
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

#Test with historical data:
if __name__ == "__main__":
    df = pd.read_csv("weather_data.csv")

    print("City Statistics:")
    print(calculate_city_statistics(df))
    print("\nCountry Statistics:")
    print(calculate_country_statistics(df))