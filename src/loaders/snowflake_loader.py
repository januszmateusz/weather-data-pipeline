"""Snowflake data loader."""
import os
from pathlib import Path
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from typing import Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class SnowflakeLoader:
    """Load data to SnowFlake."""

    def __init__(self):
        """Initialize Snowflake connection."""
        self.conn = self._create_connection()
        if self.conn is None:
            raise ConnectionError("Failed to establish Snowflake connection")
    
    def __enter__(self):
        """Context manager entry - enables 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - ensures connection is closed.
        
        Called automatically when exiting 'with' block,
        even if an exception occurred.
        """
        self.close()
        return False  # Don't suppress exceptions

    def _create_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection."""
        try:
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
            logger.info("Connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append'
    ) -> Tuple[bool, int, int]:
        """
        Load DataFrame to Snowflake table.

        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: 'append', 'replace', 'fail'

        Returns:
            (success, num_chunks, num_rows)
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping load")
            return False, 0, 0
        
        try:
            logger.info(f"Loading {len(df)} rows to {table_name}...")
            success, num_chunks, num_rows, _ = write_pandas(
                conn = self.conn,
                df=df,
                table_name=table_name,
                auto_create_table=False,
                overwrite=(if_exists == 'replace')
            )
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

        logger.info(f"Loaded {num_rows} rows to Snowflake table '{table_name}'")
        return success, num_chunks, num_rows
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
            cursor.close()
            logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
            
    
    def close(self) -> None:
        """Close Snowflake connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing: {e}")
    
#Example usage
if __name__ =="__main__":
    try:
        project_root = Path(__file__).parent.parent.parent
        csv_file = project_root / "weather_data.csv"
        #Load sample data
        df = pd.read_csv(csv_file)

        #Connect and Load to Snowflake
        df = df.rename(columns={'timestamp': 'record_timestamp'})
        df.columns = df.columns.str.upper()
        with SnowflakeLoader() as loader:
            loader.load_dataframe(df, "WEATHER_DATA")
            #Query data
            result = loader.execute_query("SELECT city, AVG(temperature) as avg_temp FROM WEATHER_DATA GROUP BY city")
            print("\n" + "="*50)
            print("Average temperature by city:")
            print("="*50)
            print(result.to_string(index=False))
            print("="*50)
        
        logger.info("Load completed successfully")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise