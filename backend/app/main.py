#backend 에서 실행
# uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 실행 명령어
from fastapi import FastAPI
from app.route import router
from fastapi.middleware.cors import CORSMiddleware
from app.database import engine
from app import models

models.Base.metadata.create_all(bind = engine)

app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_origins = ["*"],
  allow_credentials = True,
  allow_methods = ["*"],
  allow_headers = ["*"],
)

app.include_router(router)
