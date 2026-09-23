import asyncio
import sys
from pathlib import Path

from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.connectors.rss.parser import fetch_and_parse_rss


def build_bing_rss_url(query: str) -> str:
    encoded_query = quote_plus(query.strip())
    return f"https://www.bing.com/search?q={encoded_query}&format=rss"


async def main():

    url = build_bing_rss_url("business news")

    print("Bing RSS URL:")
    print(url)

    result = await fetch_and_parse_rss(
        request_id="test_bing_001",
        source="bing",
        source_type="rss",
        url=url,
    )

    print("\nParser Result:")
    print(result)

    print("\nResult type:")
    print(type(result))

    if hasattr(result, "items"):
        print("\nItems:", len(result.items))

        for item in result.items[:3]:
            print("\nTitle:", item.title)
            print("Link:", item.link)


if __name__ == "__main__":
    asyncio.run(main())