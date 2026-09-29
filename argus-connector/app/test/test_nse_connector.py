import asyncio

from app.connectors.nse.connector import NSEConnector
from app.schemas.connector import ConnectorRequest


async def main():

    request = ConnectorRequest(
        request_id="test_nse_connector_001",
        source="nse",
        source_type="nse_announcements",
        source_url="https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
        metadata={},
    )

    connector = NSEConnector()

    result = await connector.fetch(request)

    print("Status:", result.status)
    print("Request ID:", result.request_id)
    print("Item Count:", result.item_count)
    print()

    for index, item in enumerate(result.items[:5], start=1):

        print(f"Item #{index}")
        print("Source:", item.source)
        print("Source Type:", item.source_type)
        print("Title:", item.title)
        print("Subject:", item.subject)
        print("Link:", item.link)
        print("Published At:", item.published_at)
        print("-" * 60)


if __name__ == "__main__":
    asyncio.run(main())