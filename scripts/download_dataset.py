import urllib.request
import os

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
local_path = "data/raw/yellow_tripdata_2023-01.parquet"

os.makedirs("data/raw", exist_ok=True)

if not os.path.exists(local_path):
    print("Downloading dataset...")
    urllib.request.urlretrieve(url, local_path)
    print("Download complete!")
else:
    print("File already exists, skipping download.")