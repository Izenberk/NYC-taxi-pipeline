def download_parquet():
    import requests
    import os

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    output = "/opt/data/raw/yellow_tripdata_2023-01.parquet"
    os.makedirs(os.path.dirname(output), exist_ok=True)


    print("📥 Downloading Parquet...")
    response = requests.get(url)
    with open(output, "wb") as f:
        f.write(response.content)
    print("✅ Download complete!")

if __name__ == "__main__":
    download_parquet()