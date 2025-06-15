from kafka import KafkaProducer, KafkaConsumer
import json
import os
from django.conf import settings

def get_kafka_producer():
    """
    Kafka 프로듀서 인스턴스를 생성하고 반환합니다.
    프로듀서는 메시지를 Kafka 토픽으로 전송하는 역할을 합니다.
    """
    return KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_kafka_consumer(topic, group_id):
    """
    Kafka 컨슈머 인스턴스를 생성하고 반환합니다.
    컨슈머는 Kafka 토픽에서 메시지를 수신하는 역할을 합니다.
    
    Args:
        topic (str): 구독할 Kafka 토픽 이름
        group_id (str): 컨슈머 그룹 ID
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        group_id=group_id,
        auto_offset_reset='earliest',  # 가장 오래된 메시지부터 읽기
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def send_message(topic, message):
    """
    Kafka 토픽으로 메시지를 전송합니다.
    
    Args:
        topic (str): 메시지를 전송할 토픽 이름
        message (dict): 전송할 메시지 (JSON 직렬화 가능한 객체)
    """
    producer = get_kafka_producer()
    producer.send(topic, message)
    producer.flush()  # 메시지가 전송될 때까지 대기

def consume_messages(topic, group_id, callback):
    """
    Kafka 토픽에서 메시지를 수신하고 콜백 함수로 처리합니다.
    
    Args:
        topic (str): 구독할 토픽 이름
        group_id (str): 컨슈머 그룹 ID
        callback (function): 수신된 메시지를 처리할 콜백 함수
    """
    consumer = get_kafka_consumer(topic, group_id)
    for message in consumer:
        callback(message.value)  # 각 메시지에 대해 콜백 함수 실행 