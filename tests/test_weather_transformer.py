import pandas as pd
import pytest
from src.transformers.weather_transformer import (
    weather_json_to_dataframe,
    batch_transform
)

def test_weather_json_to_dataframe_valid():
    data = {
        "dt": 1690000000,
        "name":"Paris",
        "sys":{"country":"FR"},
        "main": {"temp": 22, "feels_like": 21, "temp_min": 20, "temp_max": 25, "pressure": 1012, "humidity": 60},
        "weather": [{"description": "clear sky"}],
        "wind": {"speed": 3.5},
        "clouds": {"all": 0}
    }
    df = weather_json_to_dataframe(data)

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, 'city'] == "Paris"
    assert df.loc[0, 'country'] == "FR"
    assert df.loc[0, 'temperature'] == 22

def test_batch_transform_multiple_cities():
    data_list = [
        {"dt": 1690000000, "name":"Warsaw", "main":{"temp": 10.0}},
        {"dt": 1690000000, "name":"Paris", "main":{"temp": 18.0}},
        {"dt": 1690000000, "name":"New York", "main":{"temp": 28.0}}
    ]

    df = batch_transform(data_list)

    assert len(df) == 3
    assert set(df['city']) == {"Warsaw", "Paris", "New York"}
    assert df.loc[0, 'temperature'] == 10.0

def test_empty_weather_array():
    data = {
        "dt": 1690000000,
        "name": "TestCity",
        "main": {"temp": 15.0},
        "weather": []  # Pusta lista!
    }
    
    df = weather_json_to_dataframe(data)
    
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, 'city'] == "TestCity"
    # weather_description shuld be 'Unknown' or None
    assert df.loc[0, 'weather_description'] in ['Unknown', None] or \
           pd.isna(df.loc[0, 'weather_description'])

def test_invalid_type_string():
    invalid_input = "Invalid string"

    with pytest.raises(ValueError) as exc_info:
        weather_json_to_dataframe(invalid_input)
    
    error_message = str(exc_info.value)
    assert "must be dict" in error_message
    assert "str" in error_message

def test_invalid_None_type():
    invalid_input = None

    with pytest.raises(ValueError) as exc_info:
        weather_json_to_dataframe(invalid_input)
    
    error_message = str(exc_info.value)
    assert "cannot be None or empty" in error_message