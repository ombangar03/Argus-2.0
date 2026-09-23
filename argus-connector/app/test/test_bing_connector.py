import asyncio
import sys
from pathlib import Path
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.connectors.registry import get_connector
from app.schemas.connector import ConnectorRequest


def build_bing_rss_url(query: str) -> str:
    encoded_query = quote_plus(query.strip())
    return f"https://www.bing.com/search?q={encoded_query}&format=rss"


async def main():
    query = "business news"
    url = build_bing_rss_url(query)

    request = ConnectorRequest(
        request_id="test_bing_connector_001",
        source="bing",
        source_type="rss",
        source_url=url,
        metadata={
            "topic": "business",
            "query": query,
        },
    )

    connector = get_connector(request.source_type)

    print("Connector:", connector.__class__.__name__)
    print("Source:", request.source)
    print("Source Type:", request.source_type)
    print("URL:", request.source_url)

    result = await connector.fetch(request)

    print("\nResult:")
    print("Status:", result.status)
    print("Items:", result.items_count)
    print("Error:", result.error_message)

    for item in result.items[:3]:
        print("\nTitle:", item.title)
        print("Link:", item.link)
        print("Description:", item.summary)
        print("Published:", item.published_at)


if __name__ == "__main__":
    asyncio.run(main())
