"""
Weather Data Pipeline - Version 2
With data validation and advanced transformations
"""

import pandas as pd
from datetime import datetime
from typing import List
from src.extractors.weather_api import get_current_weather, WeatherAPIError
from src.transformers.weather_transformer import batch_transform
from src.transformers.analytics import(
    calculate_city_statistics,
    detect_temperature_anomalies
)
from src.utils.validators import WeatherDataValidator
from src.utils.functional import(
    add_temperature_category,
    add_comfort_index,
    transform_weather_records,
    timeit
)
from src.utils.performance import optimizied_dataframe_memory

@timeit
def extract_wheather_data(cities: List[str]) -> List[dict]:
    """Extract weather data for multiple cities."""
    weather_data = []
    failed_cities = []

    for city in cities:
        try:
            data = get_current_weather(city)
            if data:
                weather_data.append(data)
                print(f"[OK] {city}")
        except WeatherAPIError as e:
            failed_cities.append(city)
            print(f"[FAILED] {city}: {e}")
    print(f"\nExtracted: {len(weather_data)}/{len(cities)} cities")
    return weather_data

@timeit
def transform_and_validate(weather_data: List[dict]) -> pd.DataFrame:
    #Transform to DataFrame
    df = batch_transform(weather_data)

    #Add derived columns (functional approach)
    records = df.to_dict('records')
    transformations = [add_temperature_category, add_comfort_index]
    transformed_records = transform_weather_records(records, transformations)
    df = pd.DataFrame(transformed_records)

    #Validate:
    validator = WeatherDataValidator(df)
    is_valid, errors = validator.validate_all()

    if not is_valid:
        print("\n[WARNING] Data Quality Issues:")
        for error in errors:
            print(f" = {error}")
        raise ValueError("Data validation failed")
    
    print("[OK] Data validation passed")

    #Optimize memory
    df = optimizied_dataframe_memory(df)
    return df

@timeit
def generate_analytics(df: pd.DataFrame) -> None:
    """Generate and display analytics"""
    print("\n" + "=" * 60)
    print("ANALYTICS SUMMURY")
    print("*" * 60)

    #City statistics:
    city_stats = calculate_city_statistics(df)
    print("\nCity Statisctics:")
    print(city_stats.to_string(index=False))

    #Anomalies:
    anomalies = detect_temperature_anomalies(df)
    if len(anomalies) > 0:
        print(f"\n[WARNING] Temperature Anomalies Detected ({len(anomalies)}):")
        print(anomalies[['city', 'temperature', 'deviation']].to_string(index=False))
    else:
        print("\n[OK] No temperature anomalies detected")



def run_pipeline(cities: List[str], output_file: str = "weather_data.csv") -> None:
    """
    Run the enhanced weather data pipeline.
    """

    print(f"Starting pipeline at {datetime.now()}")
    print(f"Fetching weather for {len(cities)} cities...")

    try:
        #Extract
        weather_data = extract_wheather_data(cities)

        if not weather_data:
            print("[FAILED] No data collected. Exiting.")
            return
        
        #Transform & Validate
        df = transform_and_validate(weather_data)

        #Analytics
        generate_analytics(df)

        #Load
        df.to_csv(output_file, index=False)
        print(f"\n Data saved to: {output_file}")

        print("\n" + "=" * 60)
        print("[OK] PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:
        print(f"\n[FAILED] pipeline failed: {e}")
        raise

if __name__ == "__main__":
    from src.utils.config import DEFAULT_CITIES
    run_pipeline(DEFAULT_CITIES)