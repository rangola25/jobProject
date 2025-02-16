from kafka import KafkaConsumer
import json

class KafkaJSONConsumer:
    def __init__(self, topic, bootstrap_servers):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id='my-consumer-group',
            auto_offset_reset='earliest'
        )

    def consume_messages(self):
        print("Listening for messages...")
        for message in self.consumer:
            try:
                message_value = message.value.decode('utf-8')
                json_object = json.loads(message_value)
                print(f"Received message: {json.dumps(json_object, indent=4)}")
            except Exception as e:
                print(f"Error processing message: {e}")
