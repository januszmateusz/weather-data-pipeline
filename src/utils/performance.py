"""PPerformance optimization utilities."""
import pandas as pd
from typing import Iterator
import numpy as np

def optimizied_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame memory usage.

    Args:
        df: Input DataFrame

    Returns:
        Memory-optimized DataFrmae
    """
    df_optimized = df.copy()

    #Convert object columns to category if cardinality is low
    for col in df_optimized.select_dtypes(include=['object']).columns:
        num_unique = df_optimized[col].nunique()
        num_tolal = len(df_optimized[col])

        if num_unique / num_tolal < 0.5: #Less than 50% unique values
            df_optimized[col] = df_optimized[col].astype('category')

    #Downcast numeric columns
    for col in df_optimized.select_dtypes(include=['float']).columns:
        df_optimized[col] = pd.to_numeric(
            df_optimized[col],
            downcast='float'
        )
    
    for col in df_optimized.select_dtypes(include='int').columns:
        df_optimized[col] = pd.to_numeric(
            df_optimized[col],
            downcast='integer'
        )
    
    return df_optimized

def process_large_csv_in_chunks(
    filepath: str,
    chunk_size: int = 10000,
    process_func: callable = None
) -> Iterator[pd.DataFrame]:
    """
    Proces large CSV file in chunks.

    Args:
        filepath: Path to CSV file
        chunk_size: Number of rows per chunk
        process_func: Optional processing function for each chunk
    
    Yields:
        Processed DataFrame chunks
    """

    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        if process_func:
            chunk = process_func(chunk)
        yield chunk

#Vectorized operations example
def calculate_heat_index_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Calculate heat index using vectorized operations.
    Much faster than row-by-row loops!
    """
    T = df['temperature']
    RH = df['humidity']

    #Simplified heat index formula
    HI = (
        -8.78469475556 +
        1.61139411 * T +
        2.33854883889 * RH +
        -0.14611605 * T * RH +
        -0.012308094 * T**2 +
        -0.0164248277778 * RH**2 +
        0.002211732 * T**2 * RH +
        0.00072546 * T**2 * RH +
        -0.000003582 * T**2 * RH**2
    )

    return HI.round(2)

if __name__ =='__main__':
    #Load data
    df = pd.read_csv("weather_data.csv")
    print(f"Original memoery usage: {df.memory_usage(deep=True).sum()/1024**2:.2f} MB")

    #Optimize
    df_opt = optimizied_dataframe_memory(df)
    print(f"Original memoery usage: {df_opt.memory_usage(deep=True).sum()/1024**2:.2f} MB")

    #Vectorized calculation
    df_opt['heat_index'] = calculate_heat_index_vectorized(df_opt)
    print("\nHeat Index added:")
    print(df_opt[['city', 'temperature', 'humidity', 'heat_index']].head())