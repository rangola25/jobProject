import json
from datetime import datetime
import random

class Event:
    def __init__(self, reporter_id):
        self.timestamp = datetime.now()
        self.id_reporter = reporter_id
        self.metric_id = random.randint(1, 10)
        self.metric_value = random.randint(1, 100)
        self.message = "HELLO WORLD"

    def to_dict(self):
        return {
            "timestamp": self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "id_reporter": self.id_reporter,
            "metric_id": self.metric_id,
            "metric_value": self.metric_value,
            "message": self.message
        }

    def __str__(self):
        return json.dumps(self.to_dict())

event = Event(reporter_id=1)
print(event)