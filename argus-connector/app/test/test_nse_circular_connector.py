import asyncio

from app.connectors.nse.connector import NSEConnector
from app.schemas.connector import ConnectorRequest


async def main():

    request = ConnectorRequest(
        request_id="test_nse_circular_connector_001",
        source="nse",
        source_type="nse_circular",
        source_url="https://www.nseindia.com/api/circulars",
        metadata={
            "from_date": "23-09-2026",
            "to_date": "30-09-2026",
        },
    )

    connector = NSEConnector()

    result = await connector.fetch(request)

    print("\n" + "=" * 70)
    print("CONNECTOR TEST")
    print("=" * 70)

    print("Request ID :", result.request_id)
    print("Status     :", result.status)
    print("Item Count :", result.item_count)

    print("\n" + "=" * 70)
    print("FIRST ITEM")
    print("=" * 70)

    if result.items:
        print(result.items[0].model_dump())


if __name__ == "__main__":
    asyncio.run(main())