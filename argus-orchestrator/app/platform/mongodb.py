import logging 

from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import settings

logger = logging.getLogger(__name__)

print("=======================================")
print("Initializing MongoDB client....")
print("MongoDB URL:", settings.mongo_url)
print("MongoDB Database:", settings.mongo_database)

# creates mongodb client
mongo_client = AsyncIOMotorClient(
    settings.mongo_url
)

database = mongo_client[
    settings.mongo_database
]

rss_requests = database["rss_requests"]

rss_items = database["rss_items"]


async def check_mongodb():

    print("checking mongodb connections")

    try:
        await mongo_client.admin.command("ping")  # this sends actual request to mongodb
        print("MongoDB connection successful!")

        logger.info("MongoDB connection successful.")

        return True

    except Exception as exc:
        print("MongoDB connection FAILED!")
        print("Error:", exc)


        logger.exception(
            "MongoDB connection failed"
        )

        return False