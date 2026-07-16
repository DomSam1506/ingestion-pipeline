import requests
import polars as pl
import os
import time
import io
from datetime import datetime
from google.cloud import storage

def fetch_historical_weather(lat: float, lon: float, start_date: str, end_date: str):
    """Fetches hourly weather data from Open-Meteo and strictly catches API errors."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    metrics = (
        "temperature_2m,relative_humidity_2m,precipitation,"
        "snowfall,surface_pressure,cloudcover,windspeed_10m,winddirection_10m"
    )
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": metrics,
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        # If the API returns a 400 Bad Request (like a bad date format), this triggers the except block
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # We grab the exact JSON error text from the API (e.g. {"error":true,"reason":"Invalid date format"})
        error_msg = response.text if response is not None else str(e)
        
        # CRITICAL FIX: We explicitly RAISE the exception to force Airflow to mark the task as FAILED.
        # Returning 'None' here would trick Airflow into thinking the task succeeded.
        raise Exception(f"CRITICAL API FAILURE: {error_msg}")

def process_and_upload_to_gcs(json_data, city_name: str, batch_timestamp: str, bucket_name: str):
    """Converts JSON to Polars, enforces schema, and streams directly to GCP Storage."""
    hourly_data = json_data.get("hourly", {})
    if not hourly_data:
        raise ValueError(f"No hourly data found in payload for {city_name}.")

    # 1. Process with Polars
    df = pl.DataFrame(hourly_data)
    df = df.with_columns([
        pl.col("time").cast(pl.Datetime),
        pl.col("temperature_2m").cast(pl.Float64),
        pl.col("relative_humidity_2m").cast(pl.Float64),
        pl.col("precipitation").cast(pl.Float64),
        pl.col("snowfall").cast(pl.Float64),
        pl.col("surface_pressure").cast(pl.Float64),
        pl.col("cloudcover").cast(pl.Float64),
        pl.col("windspeed_10m").cast(pl.Float64),
        pl.col("winddirection_10m").cast(pl.Float64)
    ])
    df = df.with_columns(pl.lit(city_name).alias("city"))
    
    # 2. Dynamic naming for Idempotency
    file_name = f"raw/city={city_name.lower()}/weather_{batch_timestamp}.parquet"
    
    # 3. Stream Polars DataFrame to GCS Lake using io.BytesIO buffer
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
        
    print(f"  -> Successfully uploaded {df.height} rows for {city_name}!")

def run_single_city_extraction(city: str, lat: float, lon: float, target_bucket: str, start_date: str, end_date: str):
    """Executes extraction for a single city, triggered by an individual Airflow task."""
    
    batch_ts = datetime.now().strftime("%Y%m%d_%H") 
    
    print(f"\nExtracting {city} (Lat: {lat}, Lon: {lon}) from {start_date} to {end_date}...")
    
    # Fetch Data
    raw_json = fetch_historical_weather(lat, lon, start_date, end_date)
    
    # Process and Upload
    process_and_upload_to_gcs(raw_json, city, batch_ts, target_bucket)
    print(f"\n--- {city} Ingestion Complete! ---")