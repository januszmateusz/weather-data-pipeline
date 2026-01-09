# Quick Start Guide

Get up and running with Weather Data Pipeline in 5 minutes!

## 5-Minute Setup

### Step 1: Get API Key (2 min)

1. Go to [OpenWeatherMap](https://openweathermap.org/api)
2. Click "Sign Up" (it's free!)
3. Verify your email
4. Go to "API keys" tab
5. Copy your API key

### Step 2: Install (2 min)

```bash
# Clone repository
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure (30 sec)

Create a `.env` file:

```bash
echo "WEATHER_API_KEY=your_api_key_here" > .env
```

Replace `your_api_key_here` with your actual API key!

### Step 4: Run! (30 sec)

```bash
python -m src.main
```

**Done!** Check `weather_data.csv` for results.

## Your First Weather Report

After running, you'll see output like:

```
Starting pipeline at 2024-10-05 14:30:00
Fetching weather for 12 cities...
[OK] Warsaw
[OK] Krakow
[OK] London
...
[OK] Tokyo

Transforming 12 records...
Saving to weather_data.csv...

==================================================
Pipeline completed at 2024-10-05 14:30:15
Successfully processed: 12
Failed: 0
Output saved to: weather_data.csv
==================================================
```

## Common Use Cases

### 1. Get Weather for One City

```python
from src.extractors.weather_api import get_current_weather

weather = get_current_weather("Warsaw")
print(f"Temperature: {weather['main']['temp']}°C")
```

### 2. Compare Multiple Cities

```python
from src.extractors.weather_api import get_current_weather
from src.transformers.weather_transformer import batch_transform

cities = ['Warsaw', 'London', 'Tokyo']
data = [get_current_weather(city) for city in cities]
df = batch_transform(data)

print(df[['city', 'temperature']].sort_values('temperature', ascending=False))
```

### 3. Find Warmest City

```python
import pandas as pd

df = pd.read_csv('weather_data.csv')
warmest = df.loc[df['temperature'].idxmax()]
print(f"Warmest city: {warmest['city']} at {warmest['temperature']}°C")
```

### 4. Monitor Your Cities

Edit `src/main.py` and change the city list:

```python
cities = [
    'Warsaw',      # Your hometown
    'New York',    # Where your friend lives
    'Tokyo',       # Where you're traveling
]
```

Then run: `python -m src.main`

## Troubleshooting

### Problem: "WEATHER_API_KEY is not set"

**Solution:** Make sure `.env` file exists with your API key:
```bash
cat .env  # Should show: WEATHER_API_KEY=your_key
```

### Problem: "City 'XYZ' not found"

**Solution:** Check spelling or try format `"City,CountryCode"`:
```python
get_current_weather("Paris,FR")  # Instead of just "Paris"
```

### Problem: Import errors

**Solution:** Make sure virtual environment is activated:
```bash
# You should see (venv) in your terminal
# If not, run:
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
```

## Next Steps

Now that you're up and running:

1. **Read the full [README.md](README.md)** for detailed documentation
2. **Check [CONTRIBUTING.md](CONTRIBUTING.md)** if you want to contribute
3. **Explore the code** in `src/` directory
4. **Run tests:** `pytest`
5. **Customize** for your needs!

## Pro Tips

### Run Every Hour with Cron

```bash
# crontab -e
0 * * * * cd /path/to/project && /path/to/venv/bin/python -m src.main
```

### Save to Different File

```python
from src.main import run_pipeline

cities = ['Warsaw', 'London']
run_pipeline(cities, output_file='my_weather.csv')
```

### Add Error Handling

```python
from src.extractors.weather_api import get_current_weather, WeatherAPIError

try:
    weather = get_current_weather("Warsaw")
    print(f"Temp: {weather['main']['temp']}°C")
except WeatherAPIError as e:
    print(f"Oops! {e}")
```

## Learning Path

**Beginner:**
- Run the default pipeline
- Modify city list
- Read the CSV output

**Intermediate:**
- Write custom analysis scripts
- Add new data transformations
- Schedule with cron

**Advanced:**
- Add database integration
- Create visualizations
- Contribute new features

## Need Help?

- Check [README.md](README.md) for detailed docs
- Found a bug? [Open an issue](https://github.com/yourusername/weather-data-pipeline/issues)
- Have questions? [Start a discussion](https://github.com/yourusername/weather-data-pipeline/discussions)

---

**Happy weather tracking!**