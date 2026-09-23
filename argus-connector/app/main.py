from app.workers.connector_worker import ConnectorWorker
import asyncio 
from contextlib import asynccontextmanager
from fastapi import FastAPI 

from app.core.logging import setup_logging
from app.platform.redis import check_redis
from app.workers.connector_worker import run_worker

setup_logging()

worker = ConnectorWorker(block_ms = 2000, batch_size=5)
worker_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Starts the background Redis consumer on server startup, and stops on shutdown."""
    global worker_task
    print("Argus Connector starting up on port 8001...")

    # start the rss worker loop as background task
    worker_task = asyncio.create_task(worker.start())
    yield


    #graceful shutdown
    print("Argus Connector shutting down...")
    worker.stop()
    if worker_task:
        await asyncio.gather(worker_task, return_exceptions=True)
    print("Argus Connector shutdown gracefully!")

app = FastAPI(
    title="Argus Connector",
    version="2.0",
    lifespan=lifespan
)


@app.get("/")
async def root():
    return {
        "service": "argus-connector",
        "platform": "rss",
        "status": "running"
    }


@app.get("/health")
async def health():
    redis_ok = await check_redis()
    return {
        "status": "Healthy" if redis_ok else "unhealthy",
        "redis": "connected" if redis_ok else "disconnected"
    }
    