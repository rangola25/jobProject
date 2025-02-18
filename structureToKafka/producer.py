import random
import time
from confluent_kafka import Producer
from datetime import datetime
import json

class Event:
    def __init__(self, timestamp, id_reporter, metric_id, metric_value, message):
        self.timestamp = timestamp
        self.id_reporter = id_reporter
        self.metric_id = metric_id
        self.metric_value = metric_value
        self.message = message

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "id_reporter": self.id_reporter,
            "metric_id": self.metric_id,
            "metric_value": self.metric_value,
            "message": self.message
        }

    def __str__(self):
        return json.dumps(self.to_dict(), default=str)

class KafkaProducerService:
    def __init__(self, bootstrap_servers='localhost:9093', topic='events-topic'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'python-producer',
            'message.max.bytes': 1000000000
        }
        self.producer = Producer(self.conf)
        self.id_reporter = 1
        self.event_count = 0
        self.max_events = 5  # הגבלת יצירת 5 אובייקטים

    def delivery_callback(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def create_event(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metric_id = random.randint(1, 10)
        metric_value = random.randint(1, 100)
        message = "HELLO WORLD"

        event = Event(timestamp, self.id_reporter, metric_id, metric_value, message)

        self.id_reporter += 1
        self.event_count += 1

        return event

    def send_event(self):
        event = self.create_event()
        event_json = str(event)
        self.producer.produce(self.topic, key=event.timestamp, value=event_json, callback=self.delivery_callback)
        self.producer.flush()

    def run(self):
        while self.event_count < self.max_events:  # לעצור אחרי 5 אובייקטים
            self.send_event()
            time.sleep(1)
