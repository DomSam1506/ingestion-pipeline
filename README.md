☁️ Cloud-Native Data Ingestion Pipeline

📌 Project Overview

This repository contains a containerized, highly concurrent data ingestion engine. Designed to extract, transform, and load (ETL) high-volume API payloads into a Google Cloud Storage (GCP) Data Lake, this project prioritizes memory efficiency, network resilience, and strict data quality enforcement.

Currently, the pipeline dynamically orchestrates the extraction of over 130,000+ historical weather records across 5 global cities, processing deeply nested JSON into compressed Parquet files.

Architecture & Flow

Extract: Apache Airflow dynamically generates tasks for multiple geographic targets, fetching payload data from the Open-Meteo REST API.

Transform: Polars parses the nested JSON payloads, strictly enforces data types (schema validation), and executes vectorized transformations.

Load: Transformed DataFrames are serialized into binary .parquet format and streamed directly into a GCP Data Lake via in-memory byte buffers.

Core Engineering Achievements

1. Memory-Optimized Direct-to-Cloud Streaming

The Challenge: Standard data engineering tutorials often write API payloads to a local disk before uploading to the cloud, creating severe I/O bottlenecks and potential storage limits.
The Solution: I engineered a direct-to-cloud ingestion layer using Python's io.BytesIO. Polars writes the compressed Parquet binaries directly into an in-memory buffer. By utilizing buffer.seek(0), the pipeline bypasses local disk writing entirely, streaming the finalized binary packets seamlessly to Google Cloud Storage while preventing Cannot flush without finalizing I/O conflicts.

2. Resilient Orchestration & Concurrency Management

The Challenge: Launching fully parallel extraction tasks triggered immediate 429 Too Many Requests failures from the source API.
The Solution: Instead of reverting to a slow, sequential loop, I implemented Airflow Task Pools to manage active connection slots. This preserves a parallel, dynamic task-mapping architecture while enforcing strict API rate limits. Coupled with timedelta network retries, the pipeline handles transient connection drops without failing the DAG.

3. Strict Schema Enforcement

The Challenge: Blindly trusting JSON payloads often leads to downstream Data Warehouse crashes when source APIs unexpectedly change data types.
The Solution: Leveraged Polars to explicitly define and cast the schema (e.g., pl.Float64, pl.Datetime) at the exact moment of extraction, guaranteeing data integrity before cloud serialization.

📁 Repository Structure

├── dags/

│   └── weather_ingestion_dag.py    # Airflow DAG defining dynamic tasks and pools

├── include/

│   └── extract_and_stage.py        # Core Polars & GCP boto3 extraction logic

├── dbt_transform/                  # (In Progress) Scaffold for downstream dimensional modeling

├── .env                            # Local environment variables (Git-ignored)

└── requirements.txt                # Project dependencies (Polars, DuckDB, GCS)


💻 How to Run Locally

This project uses the Astro CLI to manage the Dockerized Airflow environment.

Clone the repository:

git clone https://github.com/DomSam1506/ingestion-pipeline.git
cd ingestion-pipeline


Configure Credentials:

Create a .env file in the root directory.

Add your Google Cloud Service Account key: GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/gcp-key.json

Add your target GCP Bucket name to the Airflow UI Variables (gcp_bucket_name).

Start the Environment:

astro dev start


Trigger the Pipeline:
Navigate to http://localhost:8080, unpause the gcs_weather_ingestion_dynamic DAG, and trigger a run.
