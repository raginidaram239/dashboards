from databutton.storage_client import StorageClient

client = StorageClient()

dataframes = client.dataframes
json = client.json
text = client.text
binary = client.binary

__all__ = [
    "dataframes",
    "json",
    "binary",
    "text",
]
