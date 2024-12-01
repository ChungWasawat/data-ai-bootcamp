from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from datetime import datetime, timedelta
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    PushMessageRequest,
    FlexMessage,
    FlexContainer,
)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    "bigquery_to_line_notify",
    default_args=default_args,
    description="Run a BigQuery query, create Flex Message, and send LINE Notify",
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
) as dag:

    # Function to execute the BigQuery query
    def query_to_xcom(**kwargs):
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

        query = """
        SELECT 
            DATE(order_items.created_at) AS order_date,
            SUM(order_items.sale_price) AS total_revenue
        FROM 
            `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
        JOIN 
            `bigquery-public-data.thelook_ecommerce.orders` AS orders 
            ON order_items.order_id = orders.order_id
        WHERE 
            DATE(order_items.created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        GROUP BY 
            order_date
        ORDER BY 
            order_date ASC
        """
        records = bq_hook.get_pandas_df(sql=query)
        return records.to_dict(orient="records")

    # Function to create a Flex Message
    def create_flex_message(data):
        max_revenue = max(item["total_revenue"] for item in data)

        def generate_revenue_entry(order_date, total_revenue, max_revenue):
            percentage = int((total_revenue / max_revenue) * 100)
            return {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": f"{order_date}: ฿{total_revenue:,.0f}",
                        "size": "sm",
                        "margin": "xs",
                    },
                    {
                        "type": "box",
                        "layout": "horizontal",
                        "contents": [
                            {
                                "type": "box",
                                "layout": "vertical",
                                "contents": [],
                                "width": f"{percentage}%",
                                "height": "6px",
                                "backgroundColor": "#4CAF50",
                            }
                        ],
                        "margin": "xs",
                    },
                ],
            }

        flex_bubble = {
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": "Daily Revenue Overview",
                        "weight": "bold",
                        "size": "lg",
                        "align": "center",
                    },
                    {
                        "type": "text",
                        "text": f"as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        "weight": "bold",
                        "size": "sm",
                        "align": "center",
                    },
                    {"type": "separator", "margin": "md"},
                    {
                        "type": "box",
                        "layout": "vertical",
                        "margin": "lg",
                        "contents": [
                            generate_revenue_entry(
                                item["order_date"], item["total_revenue"], max_revenue
                            )
                            for item in data
                        ],
                    },
                ],
            },
        }

        return FlexMessage(
            alt_text="Summary Revenue", contents=FlexContainer.from_dict(flex_bubble)
        )

    # Function to send a LINE Notify
    def send_line_notify(**kwargs):
        access_token = Variable.get("CHANNEL_ACCESS_TOKEN")
        user_id = Variable.get("LINE_USER_ID")
        data = kwargs["ti"].xcom_pull(task_ids="query_bigquery")

        flex_message = create_flex_message(data)

        configuration = Configuration(access_token=access_token)
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message(
                PushMessageRequest(to=user_id, messages=[flex_message])
            )

    # Define tasks
    bigquery_task = PythonOperator(
        task_id="query_bigquery",
        python_callable=query_to_xcom,
        provide_context=True,
    )

    send_line_notify_task = PythonOperator(
        task_id="send_line_notify",
        python_callable=send_line_notify,
        provide_context=True,
    )

    # Set task dependencies
    bigquery_task >> send_line_notify_task
