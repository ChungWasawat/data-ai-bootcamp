from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
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
    'owner': 'gemini-code-assist',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
 
__doc__ = """
This DAG collects cryptocurrency price data from the CoinGecko API and sends a LINE notification with the data.

**Schedule:**

The DAG runs every 2 minutes.

**Tasks:**

1. **collect_coingecko_data_task:**
    - Fetches price data for Bitcoin, Ethereum, and Tether in USD and THB.
    - Includes market cap, 24-hour volume, 24-hour change, and last updated timestamp.
    - Stores the data in XCom.

2. **send_line_notify_task:**
    - Retrieves the data from XCom.
    - Generates a LINE Flex Message containing the cryptocurrency data.
    - Sends the Flex Message to the specified user ID via LINE Notify.

**Dependencies:**

- `collect_coingecko_data_task` runs before `send_line_notify_task`.

**Configuration:**

- Set the `CHANNEL_ACCESS_TOKEN` and `LINE_USER_ID` Airflow variables with your LINE Notify credentials.

**Notes:**

- The CoinGecko API has rate limits. Ensure you don't exceed these limits.
- The `schedule_interval` can be adjusted to suit your needs.
"""

# Define the DAG
with DAG(
    'coingecko_api_with_line_notify',
    default_args=default_args,
    description='Collect data from CoinGecko API and send LINE notify',
    schedule_interval='*/2 * * * *',  # Run every 2 minutes
    start_date=datetime(2024, 11, 20),
    catchup=False,
    doc_md = __doc__
) as dag:

    # Task 1: Collect data from CoinGecko API
    def collect_coingecko_data(**kwargs):
        api_url = 'https://api.coingecko.com/api/v3/simple/price'
        params = {
            'ids': 'bitcoin,ethereum,tether',
            'vs_currencies': 'usd,thb',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true',
        }

        response = requests.get(api_url, params=params)
        data = response.json()
        kwargs['ti'].xcom_push(key='data', value=data)


    def generate_crypto_flex(data):
        icon_img = {
            "bitcoin": "https://assets.coingecko.com/coins/images/1/standard/bitcoin.png",
            "ethereum": "https://assets.coingecko.com/coins/images/279/standard/ethereum.png",
            "tether": "https://assets.coingecko.com/coins/images/325/standard/Tether.png",
        }
        contents = []
        for name, info in data.items():
            contents.append(
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {
                            "type": "image",
                            "url": icon_img[name],
                            "size": "xs",
                            "aspectMode": "cover",
                            "aspectRatio": "1:1",
                        },
                        {
                            "type": "text",
                            "text": f"{name.capitalize()} ({name[:3].upper()})",
                            "weight": "bold",
                            "size": "md",
                            "margin": "md",
                        },
                    ],
                    "spacing": "md",
                    "margin": "lg",
                }
            )
            # Add prices
            contents.append(
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "USD:", "size": "sm", "flex": 2},
                        {
                            "type": "text",
                            "text": f"${info['usd']:,}",
                            "size": "sm",
                            "flex": 3,
                        },
                        {"type": "text", "text": "THB:", "size": "sm", "flex": 2},
                        {
                            "type": "text",
                            "text": f"฿{info['thb']:,}",
                            "size": "sm",
                            "flex": 3,
                        },
                    ],
                }
            )
            # Add 24h change
            contents.append(
                {
                    "type": "text",
                    "text": f"24h Change: {info['usd_24h_change']:.2f}%",
                    "size": "sm",
                    "color": "#00FF00" if info["usd_24h_change"] > 0 else "#FF0000",
                }
            )
            contents.append({"type": "separator", "margin": "sm"})

        # Remove the last separator
        if contents and contents[-1].get("type") == "separator":
            contents.pop()
        flex_bubles = {
            "type": "bubble",
            "size": "mega",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": "Cryptocurrency Prices",
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
                        "contents": contents,
                    },
                ],
            },
        }
        return FlexMessage(
            alt_text="Cryptocurrency Prices", contents=FlexContainer.from_dict(flex_bubles)
        )

    def send_line_notify(**kwargs):
        access_token = Variable.get("CHANNEL_ACCESS_TOKEN")
        user_id = Variable.get("LINE_USER_ID")
        crypto_data = kwargs['ti'].xcom_pull(key='data', task_ids='collect_coingecko_data_task')
        flex_message = generate_crypto_flex(crypto_data)
        
        configuration = Configuration(access_token=access_token)

        # Initialize the Messaging API client once
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message(PushMessageRequest(to=user_id, messages=[flex_message]))

        
        

    collect_coingecko_data_task = PythonOperator(
        task_id='collect_coingecko_data_task',
        python_callable=collect_coingecko_data,
    )
    
    send_line_notify_task = PythonOperator(
        task_id='send_line_notify_task',
        python_callable=send_line_notify,
    )

    collect_coingecko_data_task >> send_line_notify_task
