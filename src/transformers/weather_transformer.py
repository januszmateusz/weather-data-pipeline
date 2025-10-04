import pandas as pd
from datetime import datetime
from typing import Dict, List

def weather_json_to_dataframe(weather_data: dict) -> pd.DataFrame:
    """
    Transform weather API JSON to pandas Data Frame.
    
    Args:
        weather_data: Raw JSON from Weather API

    Returns:
        pd.DataFrame: Transformed weather data
    """

    #Extract relevant fields
    transformed = {
        'timestamp': datetime.now(),
        'city':weather_data['name'],
        'country':weather_data['sys']['country'],
        'temperature':weather_data['main']['temp'],
        'feels_like':weather_data['main']['feels_like'],
        'temp_min':weather_data['main']['temp_min'],
        'temp_max':weather_data['main']['temp_max'],
        'pressure':weather_data['main']['pressure'],
        'humidity':weather_data['main']['humidity'],
        'weather_description':weather_data['weather'][0]['description'],
        'wind_speed':weather_data['wind']['speed'],
        'clouds':weather_data['clouds']['all']
    }

    #Create DataFrame with single row
    df = pd.DataFrame([transformed])
    return df

def batch_transform(weather_data_list: List[dict]) -> pd.DataFrame:
    """
    Transform multiple weather records to single DataFrame.

    Args:
        weather_data_list: List of weather JSON objects

    Returns:
        pd.DataFrame: Combined weather data
    """

    dfs = [weather_json_to_dataframe(data) for data in weather_data_list]
    return pd.concat(dfs, ignore_index=True)

if __name__ == '__main__':
    from src.extractors.whather_api import get_current_weather 

    #Get weather for multiple cities
    cities = ['Warsaw', 'London', 'New York', 'Tokyo', 'Sydney', 'Turin']
    weather_data = [get_current_weather(city) for city in cities]

    #Transform to DataFrame
    df = batch_transform(weather_data)

    #Explore the data
    print(df.head())
    print("\nDataFrame Info:")
    print(df.info())
    print("\nStatistics:")
    print(df.describe())

    #Example: Filter cities with temp > 15 C
    warm_cities = df[df['temperature'] > 15]
    print(f"\nCities warmer than 15 C:\n{warm_cities[['city', 'temperature']]}")