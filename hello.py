import json
import numpy as np
import numpy as np
import kafka
from PyQt5.QtWidgets import QApplication
from PyQt5 import QtCore
import pyqtgraph as pg
import sys
import kafka  # or whatever your consumer lib is
from kafka import KafkaConsumer


import pyqtgraph.colormap as cm
print(cm.listMaps())


# 1) Initialize Kafka consumer once
consumer_1 = KafkaConsumer(
    'tweets_topic',  # Subscribe to the 'tweets_topic'
    bootstrap_servers="172.21.97.207:9092",
    auto_offset_reset='latest', # Start reading from the beginning if no offset is stored
    enable_auto_commit=True,     # Automatically commit offsets
    group_id='my-tweet-consumer-group', # Assign to a consumer group (important for distributed consumption)
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize the JSON bytes back to a Python dict
)

def get_matrix():
    # pull one batch off your consumer (blocking or non‑blocking)
    message = next(consumer_1)
    print("received message")
    tweet_data = message.value
    nums = []
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
    # parse it into a 100×100 array of floats in [–1,1]
    # …your code…
    return matrix

app = QApplication(sys.argv)

# Set up a window and an ImageItem
win = pg.GraphicsLayoutWidget(show=True, title="Real‑Time Heatmap")
view = win.addViewBox()
view.setAspectLocked()
img = pg.ImageItem(np.zeros((100,100)))
view.addItem(img)

# Fix the color scale
img.setLookupTable(pg.colormap.get('CET-D1').getLookupTable(nPts=256))
img.setLevels([-1, 1])

def update():
    matrix = get_matrix()
    img.setImage(matrix, autoLevels=False)

# Timer drives the event loop & update
timer = QtCore.QTimer()
timer.timeout.connect(update)
timer.start(1000)  # ms

sys.exit(app.exec_())