# kafka producer
from confluent_kafka import Producer
import os
producer = Producer({'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")})

def send_to_kafka(topic, value):
    producer.produce(topic, value.encode('utf-8'))
    producer.flush()