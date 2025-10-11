"""Functional programming utilities for data processing."""
from typing import Callable, List, Dict, Any
from functools import reduce, wraps
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

def timeit(func: Callable) -> Callable:
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f"{func.__name__} took {end - start:.4f} seconds")
        return result
    return wrapper

def retry_on_exception(max_attempts: int = 3):
    """Decorator to retry function on exception."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    logging.warning(f"Attempt {attempt + 1} failed: {e}")
                    time.sleep(2 ** attempt)

            return None
        return wrapper
    return decorator

#Pure functions fo data transformation
def normalize_temperature(temp: float, from_unit: str = 'celsius') -> float:
    """Convert temperature to Celsius."""
    if from_unit == 'fahrenheit':
        return (temp - 32) * 5/9
    elif  from_unit == 'kelvin':
        return temp - 273.15
    return temp

def categorize_temperature(temp: float) -> str:
    """Categorize temperature into ranges."""
    if temp < 0:
        return 'freezing'
    elif temp < 10:
        return 'cold'
    elif temp < 20:
        return 'mild'
    elif temp < 30:
        return 'warm'
    else:
        return "hot"

#Functional transformations
@timeit
def transform_weather_records(
    records: List[Dict[str, Any]],
    transformations: List[Callable]
) -> List[Dict[str, Any]]:
    """
    Apply a series of transformations to weather records.

    Args:
        records: List of weather dictionaries
        transformations: List of transformation functions
    
    Returns:
        Transformed records
    """

    return reduce(
        lambda recs, transform: list(map(transform, recs)),
        transformations,
        records
    )

#Example transformations
def add_temperature_category(record: Dict[str, Any]) -> Dict[str, Any]:
    return {**record, 'temp_category': categorize_temperature(record['temperature'])}

def add_comfort_index(record: Dict[str, Any]) -> Dict[str, Any]:
    temp = record['temperature']
    humidity = record['humidity']
    comfort = 100 - abs(temp - 20) * 2 - abs(humidity - 50) * 0.5
    return {**record, 'comfort_index': round(max(0, min(100, comfort)), 1)}
    
#Filter functions
def is_comfortable_weather(record: Dict[str, Any]) -> bool:
    """Filter for comfortable weather conditions."""
    return(
        10 <= record['temperature'] <= 25 and
        30 <= record['humidity'] <= 70
    )

#Example usage
if __name__ == "__main__":
    sample_records = [
        {'city': 'Warsaw', 'temperature': 15, 'humidity': 60},
        {'city': 'London', 'temperature': 8, 'humidity': 80},
        {'city': 'Sydney', 'temperature': 28, 'humidity': 45},
    ]

    #Apply transformations
    transformations = [add_temperature_category, add_comfort_index]
    transformed = transform_weather_records(sample_records, transformations)

    print("Transformed records:")
    for row in transformed:
        print(row)

    #Filter comfortable cities
    comfortable = list(filter(is_comfortable_weather, transformed))
    print(f"\nComfortable cities: {[r['city'] for r in comfortable]}")

