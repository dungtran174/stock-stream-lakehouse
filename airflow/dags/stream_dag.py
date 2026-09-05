from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from confluent_kafka import Producer
import requests

default_args = {
    "owner": "dungtran",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
    "org.apache.iceberg:iceberg-aws-bundle:1.4.2,"
    "org.apache.hadoop:hadoop-aws:3.3.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
)


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
                print("API indicated data transmission is complete. Stopping.")
                break

            params["offset"] += 100

        except requests.exceptions.RequestException as e:
            print(f"Connection error to API: {e}. Stopping.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Stopping.")
            break

    producer.flush()


with DAG(
    dag_id="stream_dag",
    default_args=default_args,
    description="Stream stock transactions from Flask API to Kafka and write into Iceberg Bronze",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    stream_task = PythonOperator(
        task_id="stream_from_api_to_kafka_task",
        python_callable=stream_data_to_kafka,
    )

    submit_streaming_job = SparkSubmitOperator(
        task_id="submit_kafka_to_iceberg_job",
        application="/opt/airflow/code/stream_kafka_iceberg.py",
        conn_id="spark_conn",
        verbose=True,
        packages=SPARK_PACKAGES,
    )

    [stream_task, submit_streaming_job]
