from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

# ตั้งค่าพารามิเตอร์เริ่มต้น
default_args = {
    "owner": "dataai",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

PROJECT_ID = "dataaibootcamp"
BUCKET_NAME = "dataai-chung-9999"


# สร้างข้อมูลตัวอย่าง
def generate_sample_data():
    n_records = 100
    data = {
        "id": range(n_records),
        "name": [f"สินค้า_{i}" for i in range(n_records)],
        "price": np.random.randint(100, 1000, n_records),
        "created_at": [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in range(n_records)
        ],
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/products.csv", index=False)


with DAG(
    dag_id="08_storage_simple_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # สร้าง Bucket ใน Cloud Storage
    create_bucket = GCSCreateBucketOperator(
        gcp_conn_id="gcp_conn",
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        location="ASIA",
    )

    generate_csv = PythonOperator(
        task_id="create_orders", python_callable=generate_sample_data
    )

    # อัพโหลดไฟล์ไปยัง Cloud Storage
    upload_to_gcs = LocalFilesystemToGCSOperator(
        gcp_conn_id="gcp_conn",
        task_id="upload_to_gcs",
        src="/tmp/products.csv",
        dst="raw/products.csv",
        bucket=BUCKET_NAME,
    )

    create_bucket >> generate_csv >> upload_to_gcs
