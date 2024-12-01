from __future__ import annotations 

import json
import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from google.cloud import storage


# Replace with your project ID, bucket name, and dataset/table name
PROJECT_ID = "DataAIBootCamp"
BUCKET_NAME = "dataai-chung-9999"
DATASET_TABLE = "gemini_assist_workshop.coingecko_price"


def collect_and_upload_data(ti):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether&vs_currencies=usd,thb&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"
    response = requests.get(url)
    data = response.json()

    # Add timestamp
    for coin, values in data.items():
        values["last_updated_at"] = datetime.fromtimestamp(
            values["last_updated_at"]
        ).strftime("%Y-%m-%d %H:%M:%S UTC")  # Format the timestamp


    temp_file = "/tmp/coingecko_data.json"
    with open(temp_file, "w") as f:
        json.dump(data, f)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/coingecko/coingecko_data.json")
    blob.upload_from_filename(temp_file)

    ti.xcom_push(key="temp_file", value=temp_file)  # push the temp file location



with DAG(
    dag_id="coingecko_api_to_bq_dag",
    schedule="@weekly", # Every Monday 21:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    owner="gemini-code-assist",  # Set DAG owner
    tags=["coingecko", "api", "bigquery"],
) as dag:
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        dataset_id=DATASET_TABLE.split(".")[0],
        table_id=DATASET_TABLE.split(".")[1],
        schema_suffix="_schema",

    )


    collect_data = PythonOperator(
        task_id="collect_and_upload_data",
        python_callable=collect_and_upload_data,
    )


    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["raw/coingecko/coingecko_data.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=DATASET_TABLE,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],

    )

    create_bq_table >> collect_data >> load_to_bq