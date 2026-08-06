from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from datetime import timedelta

from include.extract_and_stage import run_single_city_extraction

CITIES = {
    "Vancouver": {"lat": 49.26, "lon": -122.95},
    "Toronto":   {"lat": 43.70, "lon": -79.42},
    "Seattle":   {"lat": 47.60, "lon": -122.33},
    "New_York":  {"lat": 40.71, "lon": -74.01},
    "Miami":     {"lat": 25.76, "lon": -80.19}
}

@dag(
    dag_id="meteo_weather_ingestion",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "ingestion", "dynamic"]
)
def weather_pipeline():

    
    start = EmptyOperator(task_id="start")
    end= EmptyOperator(task_id="end")

    # Fetch variables
    bucket_name = Variable.get("gcp_bucket_name")
    start_date = Variable.get("weather_start_date")
    end_date = Variable.get("weather_end_date")

    # 2. Generate dynamic tasks
    for city, coords in CITIES.items():
        
        # We pass the start/end dates into the task decorator so they register inside the worker node
        @task(
            task_id=f"extract_and_load_{city.lower()}",
            pool="open_meteo_api",  # Restricts concurrency to avoid 429 API errors
            retries=3,              # Automatically try again if the API glitches
            retry_delay=timedelta(seconds=30)
        )
        def extract_and_load_city_weather(city_name: str, lat: float, lon: float, start: str, end: str):
            run_single_city_extraction(
                city=city_name,
                lat=lat,
                lon=lon,
                target_bucket=bucket_name,
                start_date=start,
                end_date=end
            )

        # Trigger the task instance
        city_task = extract_and_load_city_weather(city, coords["lat"], coords["lon"], start_date, end_date)

        # 3. Wire up the dependencies using bitshift operators
        # This forces the graph to start at the empty node, branch out to all cities, then merge at the end
        start >> city_task >> end

# Instantiate the DAG
weather_pipeline()