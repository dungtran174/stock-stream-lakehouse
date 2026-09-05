import json
import os
import time
from datetime import datetime
from confluent_kafka import Producer
import requests


def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}")


def stream_data_to_kafka():
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    flask_host = os.getenv("FLASK_HOST", "flask-api")
    flask_port = os.getenv("FLASK_PORT", "5000")

    config = {"bootstrap.servers": kafka_bootstrap, "acks": "all"}
    producer = Producer(config)
    topic = f"stock_transactions_{current_year}_{current_month}_{current_day}"
    url = f"http://{flask_host}:{flask_port}/api/get_data"
    params = {
        "year": current_year,
        "month": current_month,
        "day": current_day,
        "offset": 0,
        "limit": 100,
    }

    print(f"Starting data streaming pipeline to topic {topic}...")

    while True:
        try:
            print(f"Fetching data from API with offset: {params['offset']}")
            r = requests.get(url=url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            if data.get("status") == "error":
                print(f"API returned error: {data.get('message')}. Stopping.")
                break

            records = data.get("data")
            if records:
                key = f"{current_year}_{current_month}_{current_day}_{params['offset']}"
                value = json.dumps(records)
                producer.produce(topic, value, key, callback=delivery_callback)
                producer.poll(0)
            else:
                print("No more data received from API.")

            if data.get("status") == "complete":
                print("API completed sending records. Stopping.")
                break

            params["offset"] += 100

        except requests.exceptions.RequestException as e:
            print(f"Connection error to API: {e}. Stopping.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Stopping.")
            break

        time.sleep(1)

    producer.flush()


if __name__ == "__main__":
    stream_data_to_kafka()
