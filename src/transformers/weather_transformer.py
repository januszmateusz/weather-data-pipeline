import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

def weather_json_to_dataframe(weather_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Transform weather API JSON to pandas Data Frame.
    
    Args:
        weather_data: Raw JSON from Weather API

    Returns:
        pd.DataFrame: Transformed weather data
    """

    #Input data validation:
    if not weather_data:
        raise ValueError("weather_data cannot be None or empty")
    
    if not isinstance(weather_data, dict):
        raise ValueError(f"weather data must be dict, got {type(weather_data)}")
    

    #Extract relevant fields
    try:
        transformed = {
            'timestamp': datetime.fromtimestamp(weather_data.get('dt', datetime.now().timestamp())),
            'city': weather_data.get('name', 'Unknown'),
            'country': weather_data.get('sys', {}).get('country', 'Unknown'),
            'temperature': weather_data.get('main', {}).get('temp'),
            'feels_like': weather_data.get('main', {}).get('feels_like'),
            'temp_min': weather_data.get('main', {}).get('temp_min'),
            'temp_max': weather_data.get('main', {}).get('temp_max'),
            'pressure': weather_data.get('main', {}).get('pressure'),
            'humidity': weather_data.get('main', {}).get('humidity'),
            'weather_description': weather_data.get('weather', [{}])[0].get('description', 'Unknown'),
            'wind_speed': weather_data.get('wind', {}).get('speed'),
            'clouds': weather_data.get('clouds', {}).get('all')
        }
    except (KeyError, IndexError, TypeError) as e:
        raise ValueError(f"Invalid weather data structure: {e}")


    #Create DataFrame with single row
    df = pd.DataFrame([transformed])
    return df

def batch_transform(weather_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
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
    from src.extractors.weather_api import get_current_weather

    print("🌤️  Weather Data Transformer Test\n")
    
    cities = ['Warsaw', 'London', 'New York', 'Tokyo', 'Sydney', 'Los Angeles', 'Turin']
    
    print(f"Fetching weather for {len(cities)} cities...")
    weather_data = [get_current_weather(city) for city in cities]
    print("✓ Data fetched!\n")
    
    df = batch_transform(weather_data)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    print("="*70)
    print("DATAFRAME HEAD (First 10 rows)")
    print("="*70)
    print(df.head(10).to_string())
    
    print("\n" + "="*70)
    print("DATAFRAME INFO")
    print("="*70)
    df.info()
    
    print("\n" + "="*70)
    print("STATISTICS")
    print("="*70)
    print(df.describe().to_string())
    
    print("\n" + "="*70)
    print("CITIES WARMER THAN 15°C")
    print("="*70)
    warm_cities = df[df['temperature'] > 15]
    if len(warm_cities) > 0:
        print(warm_cities[['city', 'temperature']].to_string(index=False))
    else:
        print("No cities warmer than 15°C found.")
    
    print("\n✅ Transform test completed!")