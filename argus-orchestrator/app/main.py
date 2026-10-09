import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.api.health import router as health_router
from app.core.logging import setup_logging
from app.workers.request_worker import RequestWorker
from app.workers.result_worker import ResultWorker

from app.workers.scheduler_worker import SchedulerWorker

setup_logging()

request_worker = RequestWorker(poll_interval=2.0)
result_worker = ResultWorker(block_ms=2000, batch_size=10)
scheduler_worker = SchedulerWorker()

background_tasks = []



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start and stop the Orchestrator background workers."""

    print("Argus Orchestrator starting up on Port 8000...")

    background_tasks = [
        asyncio.create_task(request_worker.start(), name="request-worker"),
        asyncio.create_task(result_worker.start(), name="result-worker"),
        asyncio.create_task(scheduler_worker.run(), name="scheduler-worker"),
    ]

    try:
        yield
    finally:
        print("Argus Orchestrator shutting down...")

        # Signal each worker to stop.
        request_worker.stop()
        result_worker.stop()
        await scheduler_worker.stop()

        # Wait for the workers to finish their shutdown.
        await asyncio.gather(*background_tasks, return_exceptions=True)

        print("Argus Orchestrator shutdown complete.")



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
