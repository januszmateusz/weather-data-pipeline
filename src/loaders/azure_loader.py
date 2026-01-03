"""Azure Blob Storage loader."""
import os
import io
import logging
from datetime import datetime
from typing import Optional, List

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import AzureError, ResourceNotFoundError
from dotenv import load_dotenv

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class AzureBlobLoader:
    """Load data to Azure Blob Storage.
    
    This class provides methods to upload DataFrames to Azure Blob Storage
    in various formats (CSV, Parquet) and list existing blobs.
    
    Attributes:
        blob_service_client: Azure Blob Service client instance
        container_name: Name of the container to use for storage
    """

    def __init__(self):
        """Initialize Azure Blob client.
        
        Raises:
            ValueError: If AZURE_STORAGE_CONNECTION_STRING is not set
            ConnectionError: If connection to Azure fails
        """
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        if not connection_string:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING environment variable not set. "
                "Please set it in your .env file or environment."
            )
        
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            self.container_name = os.getenv('AZURE_CONTAINER_NAME', 'weather-data')
            self._ensure_container_exists()
            logger.info(f"Successfully connected to Azure Blob Storage, container: {self.container_name}")
            
        except AzureError as e:
            raise ConnectionError(f"Failed to connect to Azure Blob Storage: {e}")
    
    def _ensure_container_exists(self):
        """Ensure container exists, create if not.
        
        Raises:
            AzureError: If container creation fails
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            
            # Check if container exists
            try:
                container_client.get_container_properties()
                logger.debug(f"Container '{self.container_name}' already exists")
            except ResourceNotFoundError:
                # Container doesn't exist, create it
                container_client.create_container()
                logger.info(f"Created new container: {self.container_name}")
                
        except AzureError as e:
            logger.error(f"Error checking/creating container: {e}")
            raise
    
    def upload_dataframe_as_csv(
        self,
        df: pd.DataFrame,
        blob_name: Optional[str] = None,
        encoding: str = 'utf-8'
    ) -> str:
        """Upload DataFrame as CSV to Azure Blob Storage.

        Args:
            df: DataFrame to upload
            blob_name: Blob name (auto-generated if None)
            encoding: CSV encoding (default: utf-8)

        Returns:
            Blob URL

        Raises:
            ValueError: If DataFrame is empty
            AzureError: If upload fails
        """
        if df.empty:
            raise ValueError("Cannot upload empty DataFrame")
        
        if blob_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"weather_data_{timestamp}.csv"
        
        try:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding=encoding)
            csv_data = csv_buffer.getvalue()
            
            # Upload to blob
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_client.upload_blob(
                data=csv_data,
                overwrite=True,
                content_settings=ContentSettings(content_type='text/csv')
            )
            
            blob_url = blob_client.url
            logger.info(f"Successfully uploaded CSV to Azure Blob: {blob_name}")
            logger.debug(f"Blob URL: {blob_url}")
            
            return blob_url
            
        except AzureError as e:
            logger.error(f"Failed to upload CSV '{blob_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading CSV: {e}")
            raise

    def upload_dataframe_as_parquet(
        self,
        df: pd.DataFrame,
        blob_name: Optional[str] = None,
        compression: str = 'snappy'
    ) -> str:
        """Upload DataFrame as Parquet to Azure Blob Storage.
        
        Parquet format is more efficient for large datasets due to
        better compression and faster read/write operations.
        
        Args:
            df: DataFrame to upload
            blob_name: Blob name (auto-generated if None)
            compression: Compression type ('snappy', 'gzip', 'brotli', None)
        
        Returns:
            Blob URL
            
        Raises:
            ValueError: If DataFrame is empty
            AzureError: If upload fails
            ImportError: If pyarrow is not installed
        """
        if df.empty:
            raise ValueError("Cannot upload empty DataFrame")
        
        if blob_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"weather_data_{timestamp}.parquet"
        
        try:
            # Check if pyarrow is installed
            try:
                import pyarrow
            except ImportError:
                raise ImportError(
                    "pyarrow is required for Parquet support. "
                    "Install it with: pip install pyarrow"
                )
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(
                parquet_buffer,
                index=False,
                compression=compression,
                engine='pyarrow'
            )
            parquet_data = parquet_buffer.getvalue()
            
            # Upload to blob
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_client.upload_blob(
                data=parquet_data,
                overwrite=True,
                content_settings=ContentSettings(content_type='application/octet-stream')
            )
            
            blob_url = blob_client.url
            logger.info(f"Successfully uploaded Parquet to Azure Blob: {blob_name}")
            logger.debug(f"Blob URL: {blob_url}")
            
            return blob_url
            
        except AzureError as e:
            logger.error(f"Failed to upload Parquet '{blob_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading Parquet: {e}")
            raise
    
    def list_blobs(
        self,
        prefix: Optional[str] = None,
        max_results: Optional[int] = None
    ) -> List[str]:
        """List all blobs in container.
        
        Args:
            prefix: Filter blobs by prefix (e.g., 'weather_data_')
            max_results: Maximum number of results to return
        
        Returns:
            List of blob names
            
        Raises:
            AzureError: If listing fails
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            
            # List blobs with optional prefix
            blobs_iter = container_client.list_blobs(
                name_starts_with=prefix
            )
            
            # Convert to list with optional limit
            blob_names = []
            for i, blob in enumerate(blobs_iter):
                if max_results and i >= max_results:
                    break
                blob_names.append(blob.name)
            
            logger.info(f"Found {len(blob_names)} blobs with prefix '{prefix or ''}'")
            return blob_names
            
        except AzureError as e:
            logger.error(f"Failed to list blobs: {e}")
            raise
    
    def download_blob(
        self,
        blob_name: str,
        local_path: Optional[str] = None
    ) -> bytes:
        """Download blob from Azure Storage.
        
        Args:
            blob_name: Name of the blob to download
            local_path: Optional local path to save file
        
        Returns:
            Blob content as bytes
            
        Raises:
            ResourceNotFoundError: If blob doesn't exist
            AzureError: If download fails
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_data = blob_client.download_blob().readall()
            
            if local_path:
                with open(local_path, 'wb') as f:
                    f.write(blob_data)
                logger.info(f"Downloaded blob '{blob_name}' to '{local_path}'")
            else:
                logger.info(f"Downloaded blob '{blob_name}' to memory")
            
            return blob_data
            
        except ResourceNotFoundError:
            logger.error(f"Blob '{blob_name}' not found")
            raise
        except AzureError as e:
            logger.error(f"Failed to download blob '{blob_name}': {e}")
            raise
    
    def delete_blob(
        self,
        blob_name: str,
        delete_snapshots: bool = True
    ) -> bool:
        """Delete a blob from Azure Storage.
        
        Args:
            blob_name: Name of the blob to delete
            delete_snapshots: Whether to delete snapshots as well
        
        Returns:
            True if deletion was successful
            
        Raises:
            ResourceNotFoundError: If blob doesn't exist
            AzureError: If deletion fails
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_client.delete_blob(
                delete_snapshots='include' if delete_snapshots else 'only'
            )
            
            logger.info(f"Successfully deleted blob '{blob_name}'")
            return True
            
        except ResourceNotFoundError:
            logger.error(f"Blob '{blob_name}' not found")
            raise
        except AzureError as e:
            logger.error(f"Failed to delete blob '{blob_name}': {e}")
            raise
    
    def blob_exists(self, blob_name: str) -> bool:
        """Check if a blob exists.
        
        Args:
            blob_name: Name of the blob to check
        
        Returns:
            True if blob exists, False otherwise
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            return blob_client.exists()
        except Exception as e:
            logger.error(f"Error checking blob existence: {e}")
            return False


def main():
    """Example usage of AzureBlobLoader."""
    try:
        # Initialize loader
        loader = AzureBlobLoader()
        
        # Create sample DataFrame
        sample_data = {
            'date': pd.date_range('2024-01-01', periods=5),
            'temperature': [20.5, 21.3, 19.8, 22.1, 20.9],
            'humidity': [65, 70, 68, 72, 69],
            'location': ['Warsaw'] * 5
        }
        df = pd.DataFrame(sample_data)
        
        print("\nSample DataFrame:")
        print(df)
        print("\n" + "="*50 + "\n")
        
        # Upload as CSV
        csv_url = loader.upload_dataframe_as_csv(df)
        print(f"CSV uploaded successfully!")
        print(f"URL: {csv_url}\n")
        
        # Upload as Parquet
        parquet_url = loader.upload_dataframe_as_parquet(df)
        print(f"Parquet uploaded successfully!")
        print(f"URL: {parquet_url}\n")
        
        # List all blobs
        print("All blobs in container:")
        blobs = loader.list_blobs()
        for blob in blobs:
            print(f" - {blob}")
        
        print("\n" + "="*50 + "\n")
        
        # List only CSV files
        print("CSV files only:")
        csv_blobs = loader.list_blobs(prefix="weather_data_", max_results=5)
        for blob in csv_blobs:
            if blob.endswith('.csv'):
                print(f" - {blob}")
        
    except ValueError as e:
        print(f"Configuration error: {e}")
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()