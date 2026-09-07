from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.platform.mongodb import check_mongodb
from app.platform.redis import check_redis

router = APIRouter()

@router.get("/health")
async def health_check():
    print("health check requested")

    return {
        "status": "ok"
    }


@router.get("/ready")
async def ready_check():
    print("Ready check requested")

    mongodb_status = await check_mongodb()

    redis_status = await check_redis()

    print(f"MongoDB ready: {mongodb_status}")

    print(f"Redis ready: {redis_status}")

    if mongodb_status and redis_status:
        print("Orechestrator is READY!")

        return {
            "status": "Ready",
            "mongodb": True,
            "redis": True,
        }

    print("Orchestrator is NOT READY")

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "not_ready",
            "mongodb": mongodb_status,
            "redis": redis_status,
        },
    )