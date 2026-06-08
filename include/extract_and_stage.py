import requests
import polars as pl
import os
import time
from datetime import datetime

def fetch_historical_weather(lat: float, lon: float, start_date: str, end_date: str):
    """Fetches highly detailed hourly weather data from Open-Meteo."""
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Expanded our data columns for a much richer dataset
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
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  -> API Extraction Failed: {e}")
        return None

def process_and_save(json_data, city_name: str, batch_timestamp: str):
    """Converts JSON to Polars, enforces a wide schema, and saves as Parquet."""
    
    hourly_data = json_data.get("hourly", {})
    if not hourly_data:
        print(f"  -> Error: No hourly data found for {city_name}.")
        return

    df = pl.DataFrame(hourly_data)
    
    # Strict Schema Enforcement for all 9 API columns
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
    
    # Inject our metadata column
    df = df.with_columns(pl.lit(city_name).alias("city"))
    
    # Save to Parquet using the batch timestamp to keep file names organized
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_name = f"weather_{city_name.lower()}_{batch_timestamp}.parquet"
    file_path = os.path.join(current_dir, file_name)
    
    df.write_parquet(file_path)
    print(f"  -> Successfully processed {df.height} rows. Saved to {file_name}")

if __name__ == "__main__":
    # Dictionary of target cities to scale up our data volume
    CITIES = {
        "Vancouver": {"lat": 49.26, "lon": -122.95},
        "Toronto":   {"lat": 43.70, "lon": -79.42},
        "Seattle":   {"lat": 47.60, "lon": -122.33},
        "New_York":  {"lat": 40.71, "lon": -74.01},
        "Miami":     {"lat": 25.76, "lon": -80.19}
    }
    
    start_date = "2021-01-01"
    end_date = "2023-12-31"
    
    # Generate one timestamp for the whole batch run so the 5 files group nicely
    batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"--- Starting Multi-City Extraction Batch ({start_date} to {end_date}) ---")
    
    for city, coords in CITIES.items():
        print(f"\nExtracting data for {city}...")
        
        raw_json = fetch_historical_weather(coords["lat"], coords["lon"], start_date, end_date)
        
        if raw_json:
            process_and_save(raw_json, city_name=city, batch_timestamp=batch_ts)
            
        # Rate Limiting: Pause for 2 seconds before hitting the API again
        time.sleep(2) 
        
    print("\n--- Extraction Batch Complete! ---")