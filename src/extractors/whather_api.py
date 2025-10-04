import os
import time
import requests
from dotenv import load_dotenv
from typing import Optional

#Load environment variables
load_dotenv()

class WeatherAPIError(Exception):
    """Custom exception for weather API errors"""
    pass

def get_current_weather(
    city: str, 
    max_retries: int = 3, 
    timeout: int = 10,
    debug: bool = False
    ) -> Optional[dict]:

    """
    Fetch current weather data for a given city from OpenWeatherMap API

    Args:
        city: Name of the city to fetch weather data for (e.g. "Warsaw")
        max_retries: Maximum number of retries if the API call fails
        timeout: Timeout in seconds for the API call
        debug: Whether to print debug information

    Returns:
        dict: Weather data including temperature, humidity, and description
    
    Raises:
        WeatherAPIError: If the API call fails after max_retries
    """

    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise WeatherAPIError("WEATHER_API_KEY is not set in the environment variables")

    base_url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url, 
                params=params, 
                timeout=timeout
                )
            response.raise_for_status() 
            
            if debug:
                print(f"Request URL: {response.url}")
                print(f"Status Code: {response.status_code}")
            
            return response.json()

        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                raise WeatherAPIError(f"Timeout after {max_retries} attempts")
            time.sleep(2**attempt) #Exponential backoff: 1s, 2s, 4s

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise WeatherAPIError(f"City '{city}' not found")
            elif e.response.status_code >=500:
                print(f"Server error on attempt {attempt+1}/{max_retries}")
                if attempt == max_retries - 1:
                    raise WeatherAPIError(f"Server error after {max_retries} attempts")
                time.sleep(2 ** attempt)
            else:
                raise WeatherAPIError(f"HTTP Error: {e}")
        
        except requests.exceptions.RequestException as e:
            raise WeatherAPIError(f"Request failed: {e}")
    
    raise WeatherAPIError("Unexpected: all retries exhausted")

#TEST:
if __name__ == "__main__":
    #Test normal case
    data = get_current_weather("Warsaw", debug=True)
    print (f"Temperature in Warsaw: {data['main']['temp']}  ℃")

    #Test error case:
    try:
        data = get_current_weather("NonExistentCity1234", debug=True)
    except WeatherAPIError as e:
        print(f"Error: {e}")