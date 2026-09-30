import asyncio

from app.connectors.nse.circulars import fetch_nse_circulars


async def main():

    request_id = "test_nse_circular_api_001"

    circulars = await fetch_nse_circulars(
        request_id=request_id,
        from_date="23-09-2026",
        to_date="30-09-2026",
    )

    print()
    print("=" * 70)
    print(f"Total circular records: {len(circulars)}")
    print("=" * 70)

    for circular in circulars[:3]:

        print()
        print("=" * 70)
        print(circular.model_dump())
        print("Circular Number :", circular.circular_number)
        print("Display Number  :", circular.display_number)
        print("Date            :", circular.circular_date)
        print("Category        :", circular.category)
        print("Department      :", circular.department)
        print("Subject         :", circular.subject)
        print("File            :", circular.filename)
        print("Extension       :", circular.file_extension)
        print("Link            :", circular.file_link)


if __name__ == "__main__":
    asyncio.run(main())