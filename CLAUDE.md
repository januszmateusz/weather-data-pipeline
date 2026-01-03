# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a multi-project data engineering workspace containing two main projects:

1. **weather-data-pipeline**: Production-grade ETL pipeline for weather data with multi-cloud support (Azure, Snowflake)
2. **airflow**: Apache Airflow orchestration environment using Astronomer

## weather-data-pipeline

### Architecture

The pipeline follows a classic ETL pattern with enhanced validation and analytics:

```
Extract (API) → Transform (pandas) → Validate → Analyze → Load (CSV/Azure/Snowflake)
```

**Core Modules:**
- `src/extractors/`: API clients for data extraction (OpenWeatherMap API with retry logic)
- `src/transformers/`: Data transformation and analytics (pandas-based)
- `src/loaders/`: Multi-cloud loaders (Azure Blob Storage, Snowflake)
- `src/utils/`: Cross-cutting concerns (validation, config, functional utilities, performance optimization)

**Key Design Patterns:**
- Functional transformations via `src/utils/functional.py` for composable data enrichment
- Decorator-based timing (`@timeit`) for performance monitoring
- Context managers for resource management (Snowflake connections)
- Validator pattern for data quality (`WeatherDataValidator`)

### Running the Pipeline

```bash
# Navigate to project
cd weather-data-pipeline

# Activate virtual environment (create if needed)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add:
# - WEATHER_API_KEY (required)
# - AZURE_STORAGE_CONNECTION_STRING (optional, for Azure loader)
# - SNOWFLAKE_* credentials (optional, for Snowflake loader)

# Run main pipeline
python main.py

# Run historical collection (10 samples, 15 min intervals)
python scripts/collect_historical.py
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_weather_transformer.py -v

# Run specific test
pytest tests/test_weather_transformer.py::test_weather_json_to_dataframe_valid -v
```

### Environment Variables

**Required:**
- `WEATHER_API_KEY`: OpenWeatherMap API key

**Optional (for cloud loaders):**
- `AZURE_STORAGE_CONNECTION_STRING`: Azure connection string
- `AZURE_CONTAINER_NAME`: Azure blob container (defaults to 'weather-data')
- `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`: Snowflake credentials
- `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`: Snowflake config

### Configuration

Default configuration in `src/utils/config.py`:
- `DEFAULT_CITIES`: List of cities to monitor
- `API_TIMEOUT`: Request timeout (10 seconds)
- `MAX_RETRIES`: API retry attempts (3)
- `TEMPERATURE_UNIT`: 'metric' (Celsius), 'imperial' (Fahrenheit), or 'standard' (Kelvin)

### Multi-Cloud Loaders

**Azure Blob Storage** (`src/loaders/azure_loader.py`):
```python
from src.loaders.azure_loader import AzureBlobLoader
loader = AzureBlobLoader()
loader.upload_dataframe_as_csv(df)      # Upload as CSV
loader.upload_dataframe_as_parquet(df)  # Upload as Parquet (requires pyarrow)
loader.list_blobs()                      # List all blobs
```

**Snowflake** (`src/loaders/snowflake_loader.py`):
```python
from src.loaders.snowflake_loader import SnowflakeLoader
with SnowflakeLoader() as loader:
    # Column names must be uppercase for Snowflake
    df.columns = df.columns.str.upper()
    loader.load_dataframe(df, "WEATHER_DATA")
    results = loader.execute_query("SELECT * FROM WEATHER_DATA LIMIT 10")
```

**Important:** Snowflake requires uppercase column names. The `timestamp` column should be renamed to avoid conflicts (e.g., `RECORD_TIMESTAMP`).

### Data Validation

The pipeline includes automatic validation via `WeatherDataValidator`:
- Required columns presence
- Null value checks
- Temperature range validation (-50°C to 60°C)
- Humidity range validation (0-100%)
- Timestamp freshness (max 24 hours old)

Validation runs automatically in `main.py` after transformation. If validation fails, the pipeline raises `ValueError` with detailed error messages.

### Functional Transformations

The pipeline uses a functional approach for data enrichment (`src/utils/functional.py`):
- `add_temperature_category`: Categorizes temperatures (Cold/Mild/Warm/Hot)
- `add_comfort_index`: Calculates comfort based on temperature and humidity
- `transform_weather_records`: Applies multiple transformations in sequence

### Performance Optimization

`optimizied_dataframe_memory()` in `src/utils/performance.py` downcasts numeric types to reduce memory usage - applied automatically in the pipeline.

### Common Development Tasks

**Adding a new city:**
Edit `src/utils/config.py` and add to `DEFAULT_CITIES` list.

**Adding a new data loader:**
Create a new file in `src/loaders/` following the pattern of `azure_loader.py` or `snowflake_loader.py`. Use context managers for resource cleanup.

**Adding a new transformation:**
Add functions to `src/transformers/weather_transformer.py` for basic transforms or `src/transformers/analytics.py` for analytics. For functional transforms, add to `src/utils/functional.py`.

**Adding a new validator:**
Add methods to `WeatherDataValidator` in `src/utils/validators.py`.

## airflow

### Architecture

Standard Astronomer Airflow setup with the following components:
- Postgres: Metadata database
- Scheduler: Task monitoring and triggering
- DAG Processor: DAG parsing
- API Server: UI and API
- Triggerer: Deferred task handling

### Running Airflow

```bash
# Navigate to airflow directory
cd airflow

# Start Airflow (requires Docker)
astro dev start

# Access UI at http://localhost:8080
# Default credentials: username='admin', password='admin' (check Astronomer docs for actual defaults)

# Stop Airflow
astro dev stop

# Restart Airflow
astro dev restart

# View logs
astro dev logs

# Run tests
astro dev pytest

# Access Postgres
# Host: localhost:5432
# Database: postgres
# User: postgres
# Password: postgres
```

### Project Structure

- `dags/`: Airflow DAG definitions
  - Example DAG imports from `airflow.sdk` (Airflow 3.x+ SDK)
  - Uses `BashOperator` from `airflow.providers.standard.operators.bash`
- `tests/dags/`: DAG tests
- `Dockerfile`: Astro Runtime image configuration
- `packages.txt`: OS-level packages
- `requirements.txt`: Python packages
- `airflow_settings.yaml`: Local Airflow connections, variables, and pools
- `.astro/`: Astronomer CLI configuration

### Writing DAGs

DAGs should:
- Import from `airflow.sdk` for DAG definition (Airflow 3.x+ pattern)
- Use operators from `airflow.providers.standard.operators.*`
- Set appropriate retry logic (`retries`, `retry_delay`)
- Set `catchup=False` unless historical runs are needed
- Use task dependencies with `>>` operator

Example pattern from existing DAG:
```python
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    "dag_name",
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    task1 = BashOperator(task_id="task1", bash_command="date")
    task2 = BashOperator(task_id="task2", bash_command="sleep 5")
    task1 >> task2
```

### Adding Dependencies

**Python packages:** Add to `requirements.txt`
**OS packages:** Add to `packages.txt`
**Airflow plugins:** Add to `plugins/` directory

After adding dependencies, restart Airflow: `astro dev restart`

## General Notes

- Both projects use Python 3.8+ (weather-data-pipeline supports 3.8-3.12)
- Virtual environments are recommended for weather-data-pipeline
- Docker is required for Airflow (via Astronomer CLI)
- All credentials should be in `.env` files (never commit to version control)
- The weather pipeline includes comprehensive error handling and logging
- Tests use pytest with fixtures and mocking
