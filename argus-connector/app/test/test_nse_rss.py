import asyncio

from app.connectors.nse.rss import fetch_nse_rss


async def main():

    request_id = "test_nse_rss_001"

    announcements = await fetch_nse_rss(
        request_id=request_id,
    )

    print(f"Total announcements: {len(announcements)}")
    print()

    for index, announcement in enumerate(announcements[:5], start=1):
        print(f"Announcement #{index}")
        print("Title:", announcement.title)
        print("Subject:", announcement.subject)
        print("Link:", announcement.link)
        print("Published At:", announcement.published_at)
        print("-" * 60)


if __name__ == "__main__":
    asyncio.run(main())