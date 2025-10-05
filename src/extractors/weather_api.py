import os
import time
import requests
from dotenv import load_dotenv
from typing import Dict, Any
from src.utils.config import (
    WEATHER_API_BASE_URL,
    API_TIMEOUT,
    MAX_RETRIES,
    TEMPERATURE_UNIT
)


#Load environment variables
load_dotenv()

class WeatherAPIError(Exception):
    """Custom exception for weather API errors"""
    pass

def get_current_weather(
    city: str, 
    max_retries: int = MAX_RETRIES, 
    timeout: int = API_TIMEOUT,
    debug: bool = False
    ) -> Dict[str, Any]:

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

    Example:
        >>> weather = get_current_weather("Warsaw")
        >>> print(weather['main]['temp'])
        15.5
    """

    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise WeatherAPIError("WEATHER_API_KEY is not set in the environment variables")

    base_url = WEATHER_API_BASE_URL

    params = {
        'q': city,
        'appid': api_key,
        'units': TEMPERATURE_UNIT
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
        
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error on attempt {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                raise WeatherAPIError("Connection error. Check your internet connection.")
            time.sleep(2 ** attempt)

        except requests.exceptions.RequestException as e:
            raise WeatherAPIError(f"Request failed: {e}")

    raise WeatherAPIError("Unexpected: all retries exhausted")

#TEST:
if __name__ == "__main__":
    #Test normal case
    try:
        data = get_current_weather("Warsaw", debug=False)
        print (f"Temperature in Warsaw: {data['main']['temp']} Celsius degrees")
    except WeatherAPIError as e:
        print(f"Error: {e}")

    print("\n" + "=" * 50 + "\n") #Separator

    #Test error case:
    try:
        data = get_current_weather("NonExistentCity1234", debug=True)
    except WeatherAPIError as e:
        print(f"Error: {e}")