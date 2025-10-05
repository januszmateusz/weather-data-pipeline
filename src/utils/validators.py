"""Data validation utilities"""
import pandas as pd
from typing import List, Tuple
from datetime import datetime, timedelta

class ValidationError(Exception):
    """Raised when data validation fails."""
    pass

class WeatherDataValidator:
    """Validates weather data quality."""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.errors: List[str] = []

    def validate_required_columns(self, required_cols: List[str]) -> bool:
        """Check if all required columns exists."""
        missing = set(required_cols) - set(self.df.columns)
        if missing:
            self.errors.append(f"Missing columns: {missing}")
            return False
        return True
    
    def validate_no_nulls(self, columns: List[str]) -> bool:
        """Check for null values in specified columns."""
        has_nulls = True
        for col in columns:
            if col in self.df.columns:
                null_count = self.df[col].isnull().sum()
                if null_count > 0:
                    self.errors.append(
                        f"Column '{col}' has {null_count} null values"
                    )
                    has_nulls = False
        return has_nulls

    def validate_temperature_range(
        self,
        min_temp: float = -50,
        max_temp: float = 60
    ) -> bool:
        """Validate temperatures in within reasonable range."""
        if 'temperature' not in self.df.columns:
            return True
        out_of_range = self.df[
            (self.df['temperature'] < min_temp) | 
            (self.df['temperature'] > max_temp)]
        
        if len(out_of_range) > 0:
            self.errors.append(
                f"Found {len(out_of_range)} temperatures out of range "
                f"[{min_temp}, {max_temp}]"
            )
            return False
        return True

    def validate_humidity_range(self) -> bool:
        """Validate humidity is between 0 and 100."""
        if 'humidity' not in self.df.columns:
            return True
            
        out_of_range = self.df[
        (self.df['humidity'] < 0) | 
        (self.df['humidity'] > 100)]

        if len(out_of_range) > 0:
            self.errors.append(
            f"Found {len(out_of_range)} humidity values of range [0, 100]"
            )
            return False
        return True

    def validate_timestamp_freshness(self, max_age_hours: int = 24) -> bool:
        """Check if data is not too old"""
        if 'timestamp' not in self.df.columns:
            return True
            
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        oldest = self.df['timestamp'].min()
        age = datetime.now() - oldest

        if age > timedelta(hours=max_age_hours):
            self.errors.append(
                f"Data is  old: oldest records is {age.total_seconds()/3600:.1f} hours old"
                )
            return False
        return True
            
    def validate_all(self) -> Tuple[bool, List[str]]:
        """
        Run all validations.

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.errors = []
        required_cols = ['city', 'temperature', 'humidity', 'timestamp']

        validations = [
                    self.validate_required_columns(required_cols),
                    self.validate_no_nulls(required_cols),
                    self.validate_temperature_range(),
                    self.validate_humidity_range(),
                    self.validate_timestamp_freshness()
                ]
        is_valid = all(validations)
        return is_valid, self.errors

#Example usage:
if __name__ == "__main__":
    df = pd.read_csv("weather_data.csv")

    validator = WeatherDataValidator(df)
    is_valid, errors = validator.validate_all()
    if is_valid:
        print("✅ All validations passed!")
    else:
        print("⚠️ Validation failed:")
        for error in errors:
            print(f" - {error}")