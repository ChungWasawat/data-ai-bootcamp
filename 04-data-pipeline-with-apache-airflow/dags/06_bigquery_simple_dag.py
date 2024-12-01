import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from datetime import datetime, timedelta

default_args = {
    'owner': 'dataai',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}
PROJECT_ID = "dataaibootcamp"
DATASET_ID = "dataai_chung_9999"

with DAG(
    '06_bigquery_simple_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Create large house price table
    create_table = BigQueryCreateEmptyTableOperator(
        gcp_conn_id="gcp_conn",
        task_id='create_large_house_table',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id='house_price_large_size',
        schema_fields=[
            {'name': 'Home', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'SqFt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Bedrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Bathrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Offers', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Brick', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'Neighborhood', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        location='asia-southeast1'
    )

    # Insert data for houses > 2000 sqft
    insert_data = BigQueryInsertJobOperator(
        task_id='insert_large_houses',
        gcp_conn_id="gcp_conn",
        configuration={
            'query': {
                'query': f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.house_price_large_size`
                    SELECT 
                        Home,
                        Price,
                        SqFt,
                        Bedrooms,
                        Bathrooms,
                        Offers,
                        Brick,
                        Neighborhood
                    FROM `{PROJECT_ID}.{DATASET_ID}.house_price`
                    WHERE SqFt > 2000
                """,
                'useLegacySql': False,
                'location': 'asia-southeast1'
            }
        }
    )

    create_table >> insert_data
