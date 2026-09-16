import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.api.health import router as health_router
from app.core.logging import setup_logging
from app.workers.request_worker import RequestWorker
from app.workers.result_worker import ResultWorker

setup_logging()

request_worker = RequestWorker(poll_interval=2.0)
result_worker = ResultWorker(block_ms=2000, batch_size=10)
background_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Starts both Orchestrator workers on FastAPI startup."""
    print("🚀 Argus Orchestrator starting up on Port 8000...")

    # Launch outbound dispatcher and inbound ingestor
    t1 = asyncio.create_task(request_worker.start())
    t2 = asyncio.create_task(result_worker.start())
    background_tasks.extend([t1, t2])
    yield

    # Graceful shutdown
    print("🛑 Argus Orchestrator shutting down...")
    request_worker.stop()
    result_worker.stop()
    await asyncio.gather(*background_tasks, return_exceptions=True)


app = FastAPI(
    title="Argus Orchestrator",
    version="2.0",
    lifespan=lifespan
)

app.include_router(health_router)


@app.get("/")
async def root():
    return {
        "service": "argus-orchestrator",
        "status": "running"
    }













# async def create_index():
#     print()

#     print("Creating MongoDB Indexes....")
#     try:
#         request_index = await rss_requests.create_index(
#             "url_has",
#             unique=True
#         )

#         print("Create rss_requests index:", request_index)

#         item_index = await rss_items.create_index(
#             "item_hash",
#              unique=True
#         )

#         print("Created rss_items index:", item_index)

#         print("MongoDB index created successfully")

#     except Exception as exc:
#         print("Failed to create MongoDB indexes")

#         print("Failed to create MongoDB indexes")

#         print("Error:", exc)



# async def main():

#     setup_logging()

#     print()
#     print("======================================")
#     print("      ARGUS ORCHESTROE STARTING       ")
#     print("======================================")


#     # mongodb
#     mongo_status = await check_mongodb()

#     if mongo_status:
#         await create_index()
#     print()

#     # redis
#     redis_status = await check_redis()
#     print()


#     #-------------------------------
#     #Final status
#     # ------------------------------

#     print("================================================")
#     print("Connection Status")

#     print(
#         "MongoDB:",
#         "Connected" if mongo_status else "Failed"
#     )

#     print(
#         "Redis:",
#         "Connected" if redis_status else "Failed"
#     )

#     print("========================================================")

#     if not mongo_status or not redis_status:
#         print()

#         print(
#             "Orchestrator status FAiled"
#         )

#         print(
#             "Please check MongoDB and Redis"
#         )

#         return 

#     print()
#     print(
#         "Orchestrator foundation is healthy!"
#     )
#     print()

# if __name__ == "__main__":
#     asyncio.run(main())
