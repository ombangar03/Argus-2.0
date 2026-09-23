import asyncio
from datetime import datetime, timezone

from app.platform.mongodb import news_queries

INITIAL_QURIES = [
    {
        "topic": "business",
        "query": "business news",
    },
    {
        "topic": "business",
        "query": "corporate news",
    },
    {
        "topic": "business",
        "query": "company news"
    },
    {
        "topic": "business",
        "query": "corporate sector",
    },

]


async def seed():
    await news_queries.create_index(
        [("source", 1), ("query", 1)],
        unique=True,
    )

    for item in INITIAL_QURIES:
        now = datetime.now(timezone.utc)
        
        document = {
            "source": "bing",
            "source_type": "rss",
            "topic": item["topic"],
            "query": item["query"],
            "enabled": True,
            "priority": "HIGH",
            "metadata": {},
            "created_at": now,
            "updated_at": now
        }

        await news_queries.update_one(
            {
                "source": document["source"],
                "query": document["query"],
            },
            
            {
                "$setOnInsert": document,
            },
            upsert=True,
        )
    
    print("News queries seeded successfully.")

if __name__ == "__main__":
    asyncio.run(seed())