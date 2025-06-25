# sqlalchemy.orm 로 받아서 db에 insert
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.orm import Session
from app.schemas import ContentInput
from app import models, database

router = APIRouter()


def get_db():
  db = database.SessionLocal()
  try:
    yield db
  finally:
    db.close()


@router.post("/contents")
def create_content(data: ContentInput, db: Session = Depends(get_db)):
  exists = db.query(models.Content).filter_by(
      ctnt_name = data.ctnt_name,
      cate_name = data.cate_name,
      age_ratings = data.age_ratings).first()
  if exists:
    db.close()
    raise HTTPException(status_code = status.HTTP_409_CONFLICT,
                        detail = "중복된 데이터입니다.")
  else:
    content = models.Content(**data.dict())
    db.add(content)
    db.commit()
    db.refresh(content)
    db.close()
    return content, {"message": "데이터가 성공적으로 추가되었습니다."}
