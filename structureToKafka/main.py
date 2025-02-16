from producer import KafkaProducerService
from consumer import KafkaConsumerService

def run_producer():
    kafka_producer = KafkaProducerService()
    kafka_producer.run()

def run_consumer():
    kafka_consumer = KafkaConsumerService()
    kafka_consumer.consume_messages()

if __name__ == '__main__':
    run_producer()
    run_consumer()