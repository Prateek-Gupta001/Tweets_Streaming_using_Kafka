from kafka import KafkaConsumer
import json
import plotly.graph_objects as go
import numpy as np


# Kafka Consumer Setup
consumer_1 = KafkaConsumer(
    'tweets_topic',  # Subscribe to the 'tweets_topic'
    bootstrap_servers="172.21.97.207:9092",
    auto_offset_reset='latest', # Start reading from the beginning if no offset is stored
    enable_auto_commit=True,     # Automatically commit offsets
    group_id='my-tweet-consumer-group', # Assign to a consumer group (important for distributed consumption)
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize the JSON bytes back to a Python dict
)


def make_heatmap_figure(data: np.ndarray) -> go.Figure:
    """
    Build and return a Figure with a single Heatmap trace.
    ‑ data: 2D array of shape (n_rows, n_cols) with values in [‑1, +1]
    """
    return go.Figure(
        data=go.Heatmap(
            z=data,
            zmin=-1, zmax=1,
            colorscale='Viridis',
            colorbar=dict(title='Value')
        ),
        layout=go.Layout(
            xaxis=dict(showticklabels=False),
            yaxis=dict(showticklabels=False),
            margin=dict(l=10, r=10, t=30, b=10)
        )
    )




print("Starting Kafka consumer for tweets_topic...")



import time
try:
    print("inside try block")
    print(consumer_1)
    for message in consumer_1:
        print("received message")
        tweet_data = message.value
        nums = []
        print(f"Len Of Tweet data is {len(tweet_data)}")
        if len(tweet_data) == 32000:
            continue
        print("Types of tweet data is : ", type(tweet_data))
        print("Types of tweet data is : ", type(tweet_data[0]))
        
        for i in range(len(tweet_data)):
            nums.append(tweet_data[i].get("class"))
        window = 10
        averages = [
            sum(nums[i : i+window]) / window
            for i in range(0, len(nums), window)
        ]
        print(f"len of averages is {len(averages)}")
        arr = np.array(averages)
        # this will raise an error if arr.size != 100*100
        matrix = arr.reshape(100, 100)
        print(f"Shape of the matrix is {matrix.shape}")
        fig = make_heatmap_figure(matrix)
        fig.show()
        time.sleep(1.7)
except Exception as e:
        print(f"Got an error here {e}")
finally:
    consumer_1.close()






    
    # suppose you want to refresh every 0.5s
    

    # while True:
    #     # 1) fetch or compute your fresh batch here:
    #     #    e.g. a (50×50) array of floats between -1 and +1
    #     batch = np.random.uniform(-1, 1, size=(50, 50))

    #     # 2) build the figure
    #     fig = make_heatmap_figure(batch)

    #     # 3) show/update it
    #     #    in a script you might do fig.show()
    #     #    in Jupyter a FigureWidget would update in place
    #     fig.show()

    #     # 4) wait for the next batch
    #     time.sleep(refresh_interval)
