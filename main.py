"""
Weather Data Pipeline - Version 1
Fetches weather data for multiple cities and saves to CSV
"""

import pandas as pd
from datetime import datetime
from typing import List
from src.extractors.weather_api import get_current_weather, WeatherAPIError
from src.transformers.weather_transformer import batch_transform

def run_pipeline(cities: List[str], output_file: str = "weather_data.csv") -> None:
    """
    Run the weather data pipeline.

    Args:
    cites: List of city names to fecth weather for
    output_file: Output CSV filename
    """

    print(f"Starting pipeline at {datetime.now()}")
    print(f"Fetching weather for {len(cities)} cities...")

    weather_data = []
    failed_cities =[]

    #Extract
    for city in cities:
        try:
            data = get_current_weather(city)
            if data:
                weather_data.append(data)
                print(f"✅ {city}")
        except WeatherAPIError as e:
            print(f"❌ {city}: {e}")
            failed_cities.append(city)
    
    if not weather_data:
        print("No data collected. Exiting")
        return
    
    #Transform
    print(f"\nTransforming {len(weather_data)} records...")
    df = batch_transform(weather_data)

    #Load
    print(f"Saving t {output_file}...")
    df.to_csv(output_file, index=False)

    #Summary:
    print(f"\n{'=' * 50}")
    print(f"Pipeline completed as {datetime.now()}")
    print(f"Successfully processed: {len(weather_data)}")
    print(f"Failed: {len(failed_cities)}")
    if failed_cities:
        print(f"Failed cities: {', '.join(failed_cities)}")
    print(f"Output saved to: {output_file}")
    print(f"{'=' * 50}")

if __name__ == "__main__":
    #List of cities to monitor:
    cities = [
        'Warsaw', 'Krakow', 'Gdansk', 'Rzeszow', #Poland
        'London', 'Paris', 'Berlin',             #Europe
        'New York', 'Los Angeles', 'Chicago',    #USA
        'Tokyo', 'Sydney'                        #Asia/Oceania
    ]

    run_pipeline(cities)