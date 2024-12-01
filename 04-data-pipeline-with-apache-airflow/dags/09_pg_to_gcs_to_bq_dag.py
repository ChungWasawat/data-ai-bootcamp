import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime, timedelta

default_args = {
    "owner": "dataai",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
}

PROJECT_ID = "dataaibootcamp"
DATASET_ID = "dataai_chung_9999"
BUCKET_NAME = "dataai-chung-9999"

with DAG(
    "09_pg_to_gcs_to_bq_dag",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag:
    
    # ดึงข้อมูลจาก Postgres ไป GCS
    postgres_to_gcs = PostgresToGCSOperator(
        gcp_conn_id="gcp_conn",
        task_id="postgres_to_gcs",
        postgres_conn_id="pg_conn",
        sql="SELECT customer_id, name, join_date, loyalty_points FROM postgres.public.customers limit 1000",
        bucket=BUCKET_NAME,
        filename="customers/customers09.parquet",
        export_format="parquet",
    )

    # โหลดข้อมูลจาก GCS เข้า BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        gcp_conn_id="gcp_conn",
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["customers/*.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.customer09",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        source_format="parquet",
        create_disposition="CREATE_IF_NEEDED",
    )
    postgres_to_gcs >> gcs_to_bigquery
