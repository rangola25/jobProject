from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "eventsdb"
COLLECTION_NAME = "events"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
events_collection = db[COLLECTION_NAME]

KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "events-topic"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    group_id="your_group_id",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def save_to_mongo(event_data):
    if "timestamp" in event_data:
        timestamp_value = event_data["timestamp"]

        if isinstance(timestamp_value, int):
            event_data["timestamp"] = datetime.fromtimestamp(timestamp_value)
        else:
            try:
                event_data["timestamp"] = datetime.strptime(timestamp_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                event_data["timestamp"] = datetime.now()
    else:
        event_data["timestamp"] = datetime.now()

    events_collection.insert_one(event_data)

for message in consumer:
    event_data = message.value
    print(f"Processing event: {event_data}")

    save_to_mongo(event_data)
