from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
   
load_dotenv()
   
try:
    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    client = BlobServiceClient.from_connection_string(conn_str)
       
    # Lista kontenerów
    containers = client.list_containers()
    for container in containers:
        print(f"Found container: {container.name}")
except Exception as e:
    print(f"Error: {e}")