from kafka import KafkaProducer
import json

class KafkaMessageProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='product_views'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, message: dict):
        self.producer.send(self.topic, message)

    def flush(self):
        self.producer.flush()
