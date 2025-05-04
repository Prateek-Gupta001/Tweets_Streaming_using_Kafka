from kafka import KafkaConsumer
import json
import redis

wsl_ip_address = "172.21.97.207"



consumer_2 = KafkaConsumer(
    'tweets_topic',  # Subscribe to the *same* topic
    bootstrap_servers=f'{wsl_ip_address}:9092', # Connect to the *same* Kafka broker
    auto_offset_reset='earliest', # Can start from earliest or latest depending on need
    enable_auto_commit=True,     # Automatically commit offsets
    group_id='my-tweet-consumer-group-2', # ***THIS MUST BE DIFFERENT***
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Use the same deserializer if data format is the same
)


new_data = []

r = redis.Redis(host='localhost', port=6379, db=0)
my_keys = [1,2,3,4,5,6,7,8,9,10]

for i,message in enumerate(consumer_2):
    new_data.append(message.value)
    if i%100 == 0:
        my_keys.append(i)
        r.set(f'mydata[{i}]', json.dumps(new_data))
        print(f"Saved {i} items to Redis")

#now getting all the data stored in redis by traversing through the keys

for key in my_keys:
    print(f"Key: {key}")
    value = r.get(f'mydata[{key}]')
    print(f"Value: {value}")
