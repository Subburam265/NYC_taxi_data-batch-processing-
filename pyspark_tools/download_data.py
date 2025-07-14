import os
import pandas as pd
import requests
from pathlib import Path

DATA_DIR = Path("data/taxi_data")  # Adjust path if needed
DATA_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "yellow_tripdata_2025-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
    "yellow_tripdata_2025-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet",
    "yellow_tripdata_2025-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-03.parquet",
}

def download_taxi_data(filename: str, url: str):
    dest_path = DATA_DIR / filename
    if dest_path.exists():
        print(f"✔ {filename} already exists. Skipping download.")
        return

    print(f"↓ Downloading {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"✔ Downloaded {filename} to {dest_path}")
    except Exception as e:
        print(f"✖ Error downloading {filename}: {e}")

def download_enrichment_data():
    ENRICHMENT_DIR = Path("data/enrichment_data")
    ENRICHMENT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 1. WEATHER DATA
        print("[INFO] Downloading weather data...")
        weather_url = (
            "https://archive-api.open-meteo.com/v1/archive"
            "?latitude=40.7128&longitude=-74.0060"
            "&start_date=2025-01-01&end_date=2025-03-31"
            "&hourly=temperature_2m,precipitation,windspeed_10m"
            "&timezone=America%2FNew_York"
        )
        weather_resp = requests.get(weather_url)
        weather_resp.raise_for_status()
        weather_data = weather_resp.json()["hourly"]

        weather_df = pd.DataFrame({
            "time": weather_data["time"],
            "temperature": weather_data["temperature_2m"],
            "precipitation": weather_data["precipitation"],
            "windspeed": weather_data["windspeed_10m"],
        })
        weather_df.to_csv(ENRICHMENT_DIR / "sample_weather_q1_2025.csv", index=False)
        print("[SUCCESS] Weather data saved.")

        # 2. TRAFFIC DATA
        print("[INFO] Downloading traffic data...")
        traffic_url = (
            "https://data.cityofnewyork.us/resource/i4gi-tjb9.csv?"
            "$where=data_as_of between '2025-01-01T00:00:00' and '2025-03-31T23:59:59'"
        )
        traffic_df = pd.read_csv(traffic_url)
        traffic_df.to_csv(ENRICHMENT_DIR / "sample_traffic_q1_2025.csv", index=False)
        print("[SUCCESS] Traffic data saved.")

        # 3. TAXI ZONE LOOKUP
        print("[INFO] Downloading taxi zone lookup...")
        taxi_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        taxi_df = pd.read_csv(taxi_url)
        taxi_df.to_csv(ENRICHMENT_DIR / "sample_taxi_zone_lookup.csv", index=False)
        print("[SUCCESS] Taxi zone lookup data saved.")

    except Exception as e:
        print(f"[ERROR] Failed to fetch enrichment data: {e}")

if __name__ == "__main__":
    for fname, url in FILES.items():
        download_taxi_data(fname, url)
    download_enrichment_data()

