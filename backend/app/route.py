from fastapi import APIRouter

router = APIRouter()

@router.get("/api/hello")
async def say_hello():
    return {"message": "Hello from FastAPI!"}
