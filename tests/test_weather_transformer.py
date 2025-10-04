import pandas as pd
from src.transformers.weather_transformer import weather_json_to_dataframe

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