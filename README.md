# 🌤️ Weather Data Pipeline

> Production-grade multi-cloud ETL system built with Python, Snowflake, and Azure

[![Tests](https://img.shields.io/badge/tests-passing-success)]
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

Weather Data Pipeline is a Python-based ETL (Extract, Transform, Load) system that:
- **Extracts** real-time weather data from OpenWeatherMap API
- **Transforms** raw JSON into structured pandas DataFrames
- **Loads** processed data into CSV files for analysis

Perfect for weather monitoring, data analysis projects, or learning ETL patterns.

## ✨ Features

- ✅ **Robust API Integration** - Smart retry logic with exponential backoff
- ✅ **Error Handling** - Comprehensive exception handling for network issues
- ✅ **Batch Processing** - Fetch weather for multiple cities efficiently
- ✅ **Data Transformation** - Clean JSON-to-DataFrame conversion
- ✅ **Type Safety** - Full type hints for better IDE support
- ✅ **Configurable** - Adjustable timeouts, retries, and debug modes
- ✅ **Production Ready** - Includes logging, validation, and error recovery

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Extract   │── ──▶│  Transform   │─────▶│    Load     │
│  (API Call) │      │ (JSON → DF)  │      │ (CSV Save)  │
└─────────────┘      └──────────────┘      └─────────────┘
```

**ETL Flow:**
1. **Extract**: `weather_api.py` → Fetch data from OpenWeatherMap
2. **Transform**: `weather_transformer.py` → Convert JSON to pandas DataFrame
3. **Load**: `main.py` → Save to CSV file

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- OpenWeatherMap API key ([Get one free here](https://openweathermap.org/api))

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline
```

### Step 2: Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# .env
WEATHER_API_KEY=your_openweathermap_api_key_here
```

**Security Note:** Never commit `.env` to version control!

## ⚙️ Configuration

### Environment Variables

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `WEATHER_API_KEY` | ✅ Yes | OpenWeatherMap API key | `abc123def456...` |

### API Parameters

Configure in function calls:

```python
get_current_weather(
    city="Warsaw",
    max_retries=3,      # Number of retry attempts
    timeout=10,         # Request timeout in seconds
    debug=False         # Enable debug output
)
```

## 📖 Usage

### Quick Start

Run the complete pipeline:

```bash
python -m src.main
```

This will:
- Fetch weather for predefined cities
- Transform data to DataFrame
- Save to `weather_data.csv`

### Basic Examples

#### 1. Fetch Weather for a Single City

```python
from src.extractors.weather_api import get_current_weather

# Get current weather
weather = get_current_weather("Warsaw")
print(f"Temperature: {weather['main']['temp']}°C")
print(f"Conditions: {weather['weather'][0]['description']}")
```

#### 2. Fetch Weather for Multiple Cities

```python
from src.extractors.weather_api import get_current_weather
from src.transformers.weather_transformer import batch_transform

# Fetch data
cities = ['Warsaw', 'London', 'Tokyo']
weather_data = [get_current_weather(city) for city in cities]

# Transform to DataFrame
df = batch_transform(weather_data)
print(df[['city', 'temperature', 'humidity']])
```

#### 3. Custom Pipeline with Error Handling

```python
from src.extractors.weather_api import get_current_weather, WeatherAPIError
from src.transformers.weather_transformer import batch_transform

cities = ['Warsaw', 'InvalidCity', 'London']
weather_data = []

for city in cities:
    try:
        data = get_current_weather(city, debug=True)
        weather_data.append(data)
    except WeatherAPIError as e:
        print(f"Failed to fetch {city}: {e}")

if weather_data:
    df = batch_transform(weather_data)
    df.to_csv('my_weather.csv', index=False)
```

#### 4. Analyze Weather Data

```python
import pandas as pd

# Load saved data
df = pd.read_csv('weather_data.csv')

# Find warmest city
warmest = df.loc[df['temperature'].idxmax()]
print(f"Warmest: {warmest['city']} at {warmest['temperature']}°C")

# Cities above 20°C
hot_cities = df[df['temperature'] > 20]
print(hot_cities[['city', 'temperature', 'humidity']])

# Average temperature by country
avg_temp = df.groupby('country')['temperature'].mean()
print(avg_temp)
```

### Advanced Usage

#### Custom City Lists

Edit `main.py` to monitor your cities:

```python
cities = [
    'Warsaw', 'Krakow',           # Poland
    'Berlin', 'Munich',            # Germany
    'Paris', 'Lyon',               # France
    'New York', 'San Francisco'   # USA
]
```

#### Scheduling with Cron

Run every hour:

```bash
# crontab -e
0 * * * * cd /path/to/project && /path/to/venv/bin/python -m src.main >> logs/pipeline.log 2>&1
```

#### Integration with Databases

```python
import sqlite3
from src.main import run_pipeline

# Modify run_pipeline to save to SQLite
df.to_sql('weather_data', con=sqlite3.connect('weather.db'), 
          if_exists='append', index=False)
```

## 📁 Project Structure

```
weather-data-pipeline/
│
├── src/
│   ├── __init__.py
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── weather_api.py          # API client with retry logic
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── weather_transformer.py  # JSON to DataFrame transformer
│   └── main.py                      # Main pipeline orchestrator
│
├── tests/
│   ├── __init__.py
│   ├── test_weather_api.py
│   └── test_weather_transformer.py
│
├── .env.example                     # Example environment file
├── .gitignore
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── weather_data.csv                 # Output file (generated)
```

## 📚 API Reference

### `get_current_weather()`

Fetch current weather for a city.

```python
def get_current_weather(
    city: str,
    max_retries: int = 3,
    timeout: int = 10,
    debug: bool = False
) -> Dict[str, Any]:
```

**Parameters:**
- `city` (str): City name (e.g., "Warsaw", "London")
- `max_retries` (int): Maximum retry attempts on failure
- `timeout` (int): Request timeout in seconds
- `debug` (bool): Enable debug output

**Returns:**
- `Dict[str, Any]`: Weather data from OpenWeatherMap API

**Raises:**
- `WeatherAPIError`: On API failures or network issues

### `weather_json_to_dataframe()`

Transform single weather JSON to DataFrame.

```python
def weather_json_to_dataframe(
    weather_data: Dict[str, Any]
) -> pd.DataFrame:
```

**Parameters:**
- `weather_data` (dict): Raw JSON from Weather API

**Returns:**
- `pd.DataFrame`: Transformed weather data with columns:
  - `timestamp`, `city`, `country`, `temperature`, `feels_like`
  - `temp_min`, `temp_max`, `pressure`, `humidity`
  - `weather_description`, `wind_speed`, `clouds`

### `batch_transform()`

Transform multiple weather records.

```python
def batch_transform(
    weather_data_list: List[Dict[str, Any]]
) -> pd.DataFrame:
```

**Parameters:**
- `weather_data_list` (list): List of weather JSON objects

**Returns:**
- `pd.DataFrame`: Combined weather data

## 🧪 Testing

Run tests with pytest:

```bash
# Install pytest
pip install pytest pytest-cov

# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_weather_api.py -v
```

### Example Test

```python
def test_get_weather_success():
    weather = get_current_weather("Warsaw")
    assert 'main' in weather
    assert 'temp' in weather['main']
```

## 🔧 Troubleshooting

### Common Issues

#### 1. API Key Error

**Error:** `WEATHER_API_KEY is not set in the environment variables`

**Solution:**
- Check `.env` file exists in project root
- Verify API key is correct
- Ensure `python-dotenv` is installed

#### 2. City Not Found

**Error:** `City 'XYZ' not found`

**Solution:**
- Check city name spelling
- Try format: "City,CountryCode" (e.g., "Paris,FR")
- Use English city names

#### 3. Rate Limit Exceeded

**Error:** API returns 429 status

**Solution:**
- Free tier: 60 calls/minute, 1000 calls/day
- Add delay between requests
- Upgrade OpenWeatherMap plan

#### 4. Connection Timeout

**Error:** `Timeout after 3 attempts`

**Solution:**
- Check internet connection
- Increase timeout: `get_current_weather(city, timeout=30)`
- Check firewall/proxy settings

### Debug Mode

Enable debug output:

```python
weather = get_current_weather("Warsaw", debug=True)
```

## 📊 Output Data Format

CSV columns:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `timestamp` | datetime | Data fetch time | `2024-10-05 14:30:00` |
| `city` | string | City name | `Warsaw` |
| `country` | string | Country code | `PL` |
| `temperature` | float | Current temp (°C) | `15.5` |
| `feels_like` | float | Perceived temp (°C) | `14.2` |
| `temp_min` | float | Min temp (°C) | `13.0` |
| `temp_max` | float | Max temp (°C) | `18.0` |
| `pressure` | int | Pressure (hPa) | `1013` |
| `humidity` | int | Humidity (%) | `65` |
| `weather_description` | string | Conditions | `clear sky` |
| `wind_speed` | float | Wind speed (m/s) | `3.5` |
| `clouds` | int | Cloudiness (%) | `20` |

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [OpenWeatherMap](https://openweathermap.org/) for providing weather API
- [pandas](https://pandas.pydata.org/) for data manipulation
- [requests](https://requests.readthedocs.io/) for HTTP library

## 📧 Contact

Your Name - [@yourtwitter](https://twitter.com/yourtwitter)

Project Link: [https://github.com/yourusername/weather-data-pipeline](https://github.com/yourusername/weather-data-pipeline)

---

**Made with ❤️ and ☕ in Poland**