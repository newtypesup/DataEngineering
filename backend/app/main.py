from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .route import router

app = FastAPI(
    title="FastAPI App",
    description="FastAPI Application with Kafka and PostgreSQL",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000/"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

# @app.get("/api/hello")
# async def say_hello():
#     return {"message": "Hello from FastAPI!"}

# @app.get("/")
# async def root():
#     return {"message": "Welcome to FastAPI!"}

# @app.get("/health")
# async def health_check():
#     return {"status": "healthy"} 