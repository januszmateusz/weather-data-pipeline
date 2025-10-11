f"""Script to collect historical weather data."""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import time
from datetime import datetime
import pandas as pd
from typing import List
from main import extract_wheather_data, transform_and_validate
from src.utils.config import DEFAULT_CITIES

def collect_multiple_samples(
    cities: List[str],
    nume_samples: int = 10,
    interval_minutes: int =  30    
) -> None:

    """
    Collect weather data at regular intervals.

    Args:
        cities: List of cities to monitor
        num_samples: Number of samples to collect
        interval_minutes: Minutes between samples
    """

    output_dir = Path("data/historical")
    output_dir.mkdir(parents=True, exist_ok=True)
    all_data = []

    for i in range(nume_samples):
        print(f"\n{'=' * 60}")
        print(f"Sample {i + 1}/{nume_samples} - {datetime.now()}")
        print(f"{'=' * 60}")

        try:
            #Extract
            weather_data = extract_wheather_data(cities)

            #Transform
            df = transform_and_validate(weather_data)

            #Add sample number
            df['sample_id'] = i + 1
            all_data.append(df)

            #Save individual sample
            sample_file = output_dir / f"sample_{i + 1:03d}.csv"
            df.to_csv(sample_file, index=False)
            print(f"Sample saved to {sample_file}")

            #Wait before next sample (except last one)
            if i < nume_samples  - 1:
                wait_seconds = interval_minutes * 60
                print(f"Waiting {interval_minutes} minutes until next sample...")
                time.sleep(wait_seconds)
        except Exception as e:
            print(f"Sample {i+1} failed: {e}")
            continue
    
    #Combine all samples
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_file= output_dir /"combined_historical.csv"
        combined_df.to_csv(combined_file, index=False)

        print(f"\n{'=' * 60}")
        print(f"Historical data collection complete!")
        print(f"Total records: {len(combined_df)}")
        print(f"{'='*60}")

if __name__ == '__main__':
    #Collect 10 samples, 15 minutes apart (2.5 hours total)
    collect_multiple_samples(
        cities=DEFAULT_CITIES,
        nume_samples=10,
        interval_minutes=15
    )