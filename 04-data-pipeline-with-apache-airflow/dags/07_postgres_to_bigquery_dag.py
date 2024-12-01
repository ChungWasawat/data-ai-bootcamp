import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import json
from dotenv import load_dotenv
_ = load_dotenv()

default_args = {
    'owner': 'dataai',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

PROJECT_ID = "dataaibootcamp"
DATASET_ID = "dataai_chung_9999"

def extract(**context):
    postgres_hook = PostgresHook(postgres_conn_id='pg_conn')
    df = postgres_hook.get_pandas_df(sql='SELECT * FROM postgres.public.customers limit 100')
    
    # แปลง DataFrame เป็น JSON
    json_data = df.to_json(orient='records')
    
    # เก็บข้อมูลใน XCOM
    context['task_instance'].xcom_push(key='customer_data', value=json_data)

def transform(**context):
    # ดึงข้อมูลจาก XCOM
    json_data = context['task_instance'].xcom_pull(key='customer_data', task_ids='extract_data')
    
    # แปลง JSON กลับเป็น DataFrame
    df = pd.read_json(json_data, orient='records')
    
    # ตัวอย่างการ transform ข้อมูล
    df['first_name'] = df["name"].apply(lambda x: x.split()[0])
    df['last_name'] = df["name"].apply(lambda x: x.split()[-1])
    df["created_at"] = df["join_date"]
    df["full_name"] = df["name"]
    df['email_domain'] = df['email'].apply(lambda x: x.split('@')[-1])

    df = df[['customer_id', 'first_name', 'last_name', 'email', 'created_at', 'full_name', 'email_domain']] 
    
    # แปลงกลับเป็น JSON และเก็บใน XCOM
    transformed_json = df.to_json(orient='records')
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_json)

def load(**context):
    # ดึงข้อมูลที่ transform แล้วจาก XCOM
    json_data = context['task_instance'].xcom_pull(key='transformed_data', task_ids='transform_data')
    
    # แปลง JSON กลับเป็น DataFrame
    df = pd.read_json(json_data, orient='records')
    
    # โหลดเข้า BigQuery
    df.to_gbq(
        destination_table=f'{DATASET_ID}.customers',
        project_id=PROJECT_ID,
        if_exists='append',
        location='asia-southeast1'
    )

with DAG(
    '07_postgres_to_bigquery_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # สร้างตารางใน BigQuery
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id='customers',
        schema_fields=[
            {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email_domain', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        location='asia-southeast1'
    )

    # Extract task
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    # Transform task
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    # Load task
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load
    )

    create_table >> extract_data >> transform_data >> load_data