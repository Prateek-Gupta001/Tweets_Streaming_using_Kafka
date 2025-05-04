from kafka import KafkaConsumer
import json


wsl_ip_address = "172.21.97.207"



consumer_2 = KafkaConsumer(
    'tweets_topic',  # Subscribe to the *same* topic
    bootstrap_servers=f'{wsl_ip_address}:9092', # Connect to the *same* Kafka broker
    auto_offset_reset='earliest', # Can start from earliest or latest depending on need
    enable_auto_commit=True,     # Automatically commit offsets
    group_id='my-tweet-consumer-group-2', # ***THIS MUST BE DIFFERENT***
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Use the same deserializer if data format is the same
)
