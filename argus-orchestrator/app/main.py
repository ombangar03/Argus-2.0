# import asyncio
from fastapi import FastAPI

from app.api.health import router as health_router
from app.core.logging import setup_logging


# from app.platform.mongodb import check_mongodb, rss_requests, rss_items
# from app.core.logging import setup_logging
# from app.platform.mongodb import check_mongodb
# from app.platform.redis import check_redis


setup_logging()

app = FastAPI(
    title="Argus Orchestrator",
    version="2.0"
)

app.include_router(
    health_router
)

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
