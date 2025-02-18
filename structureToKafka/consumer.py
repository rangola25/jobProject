from confluent_kafka import Consumer, KafkaException, KafkaError

class KafkaConsumerService:
    def __init__(self, bootstrap_servers='localhost:9093', topic='events-topic'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'python-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(self.conf)

    def consume_messages(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:

                        print(f"End of partition reached {msg.partition}, offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    print(f"Consumed message: {msg.value().decode('utf-8')}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
