from confluent_kafka import Producer
import json
import time

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}")

if __name__ == '__main__':
    config = {
        # Nếu chạy script từ ngoài host, kết nối qua localhost:9092
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all'
    }
    producer = Producer(config)
    topic = 'test_topic'

    print("Bắt đầu gửi các message test lên Kafka...")
    for i in range(5):
        key = f"key_{i}"
        value = json.dumps({"message": "Hello from LakeStream", "id": i})
        producer.produce(topic, value, key, callback=delivery_callback)
        producer.poll(0)
        time.sleep(1)

    producer.flush()
    print("Đã hoàn thành gửi test data!")
