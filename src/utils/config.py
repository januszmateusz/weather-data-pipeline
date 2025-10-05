"""Configuration constants for the weather pipeline."""

# API Configuration
WEATHER_API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
API_TIMEOUT = 10    # seconds
MAX_RETRIES = 3     # number of retry attempts

# Data Configuration:
TEMPERATURE_UNIT = 'metric' # Options: 'metric', 'imperial', 'standard'
DEFAULT_CITIES = [
        'Warsaw', 'Krakow', 'Gdansk', 'Rzeszow',
        'London', 'Paris', 'Berlin',
        'New York', 'Los Angeles', 'Chicago',
        'Tokyo', 'Sydney'
    ]

# Output Configuration:
DEFAULT_OUTPUT_FILE = "weather_data.csv"
OUTPUT_DIRECTORY = "data"
LOG_DIRECTORY = "logs"

# Validation:
assert API_TIMEOUT > 0, "API_TIMEOUT must be positive"
assert MAX_RETRIES >= 1, "MAX_RETRIES must be at least 1" 
assert TEMPERATURE_UNIT in ['metric', 'imperial', 'standard']