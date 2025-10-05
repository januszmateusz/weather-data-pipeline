"""Configuration constants for the weather pipeline."""

#API Configuration
WEATHER_API_BASE_URL="https://api.openweathermap.org/data/2.5/weather"
API_TIMEOUT = 10
MAX_RETRIES = 3

#Data Configuration:
TEMPERATURE_UNIT = 'metric'
DEFAULT_CITIES = [
        'Warsaw', 'Krakow', 'Gdansk', 'Rzeszow',
        'London', 'Paris', 'Berlin',
        'New York', 'Los Angeles', 'Chicago',
        'Tokyo', 'Sydney'
    ]

#Output Configuration
DEFAULT_OUTPUT_FILE = "weather_data.csv"