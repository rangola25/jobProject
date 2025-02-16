from consumer import KafkaJSONConsumer

class Main:
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        self.topic = 'events-topic'

    def start_consumer(self):
        consumer = KafkaJSONConsumer(self.topic, self.bootstrap_servers)
        consumer.consume_messages()

if __name__ == '__main__':
    main = Main()
    main.start_consumer()