from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time
import requests

def on_send_success(record_metadata):
    print(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error delivering message: {excp}")


producer = KafkaProducer(
    bootstrap_servers='172.21.97.207:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8'), # Serialize tweet data to JSON bytes
    max_request_size=24013390
)

print("Kafka Producer has been intialised Now we will stream tweets to Kafka...")

try:
    while True:
        data = requests.get(url="http://127.0.0.1:8000/data")
        print("len of data is : ", len(data.json()))
        print("Sending data to Kafka..")
        try:
            producer.send('tweets_topic', value=data.json()).add_callback(on_send_success).add_errback(on_send_error)
        except Exception as e:
            print(f"An error occurred: {e}")
        # Simulate streaming delay
        time.sleep(1)

except Exception as e:
    print(f"An error occurred: {e}")


