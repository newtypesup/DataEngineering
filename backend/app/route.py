# router 설정
from fastapi import APIRouter, HTTPException, status
from app.schemas import ContentInput
from .kafka_producer import send_to_kafka  # 새로 추가할 producer 모듈

router = APIRouter()

@router.post("/contents")
def create_content(data: ContentInput):
    # Kafka로 메시지 전송
    send_to_kafka("contents_topic", data.json())
    return {"message": "요청이 큐에 등록되었습니다."}
