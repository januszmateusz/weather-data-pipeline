import os
import requests
from dotenv import load_dotenv

#Load environment variables
load_dotenv()

def get_current_weather(city: str, debug: bool = False) -> dict:
    """
    Fetch current weather data for a given city from OpenWeatherMap API

    Args:
        city: Name of the city to fetch weather data for (e.g. "Warsaw")

    Returns:
        dict: Weather data including temperature, humidity, and description
    """
    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError("WEATHER_API_KEY is not set in the environment variables")

    base_url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }

    response = requests.get(base_url, params=params, timeout=10)

    if debug:
        print(f"Request URL: {response.url}")
        print(f"Status Code: {response.status_code}")

    response.raise_for_status() #Raise exception for bad status codes

    return response.json()

#TEST:
if __name__ == "__main__":
    weather_data = get_current_weather("Warsaw", debug=True)
    print(weather_data)