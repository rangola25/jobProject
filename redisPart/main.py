import threading
import time
from datetime import datetime
from pymongo import MongoClient
import redis
import json
from bson import ObjectId

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "eventsdb"
COLLECTION_NAME = "events"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
events_collection = db[COLLECTION_NAME]

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def generate_redis_key(reporter_id, timestamp):
    return f"{reporter_id}:{timestamp.strftime('%d/%m/%Y:%H:%M:%S')}"

def get_last_timestamp_from_redis():
    keys = redis_client.keys('*')
    timestamps = []

    for key in keys:
        try:
            reporter_id, timestamp_str = key.split(':')
            timestamp = datetime.strptime(timestamp_str, '%d/%m/%Y:%H:%M:%S')
            timestamps.append(timestamp)
        except Exception as e:
            continue

    return max(timestamps) if timestamps else None

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

def load_to_redis():
    print("Starting to load data from MongoDB to Redis....")

    last_timestamp = get_last_timestamp_from_redis()

    query = {}
    if last_timestamp:
        query = {"timestamp": {"$gt": last_timestamp}}

    events = events_collection.find(query)

    for event in events:
        reporter_id = event.get("id_reporter")
        timestamp = event["timestamp"]

        if isinstance(timestamp, datetime):
            redis_key = generate_redis_key(reporter_id, timestamp)

            event_json = json.dumps(event, default=json_serializer)
            redis_client.set(redis_key, event_json)
            print(f"Stored in Redis: {redis_key}")

def periodic_task():
    while True:
        load_to_redis()
        time.sleep(30)

if __name__ == "__main__":
    thread = threading.Thread(target=periodic_task)
    thread.daemon = True
    thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting...")
