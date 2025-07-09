# kafka consumer
from confluent_kafka import Consumer
import psycopg2
import os
import json
consumer = Consumer({
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    'group.id': 'newtypesup_DE',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['contents_topic'])

conn = psycopg2.connect("dbname=fastapi user=fastapi password=12345 host=fastapi-db")
cur = conn.cursor()

while True:
    msg = consumer.poll(1.0)
    print("폴링 시도")
    if msg is None:
        print("메시지 없음")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print("메시지 수신:", msg.value())
    try:
        data = json.loads(msg.value().decode('utf-8'))
        cur.execute(
            "INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES (%s, %s, %s)",
            (data['ctnt_name'], data['cate_name'], data['age_ratings'])
        )
        conn.commit()
        print("DB 저장 성공:", data)
    except Exception as e:
        print("DB 저장 실패:", e)