from collections import Counter

import asyncio

from app.connectors.nse.circulars import fetch_nse_circulars


REQUEST_ID = "test_nse_circular_duplicates_001"


async def main():
    records = await fetch_nse_circulars(
        request_id=REQUEST_ID,
        from_date="23-09-2026",
        to_date="30-09-2026",
    )

    print("\n" + "=" * 70)
    print(f"TOTAL RECORDS: {len(records)}")
    print("=" * 70)

    # ---------------------------------------------------------
    # 1. Duplicate circular numbers
    # ---------------------------------------------------------

    circular_numbers = [
        record.circular_number
        for record in records
        if record.circular_number
    ]

    number_counts = Counter(circular_numbers)

    duplicate_numbers = {
        number: count
        for number, count in number_counts.items()
        if count > 1
    }

    print("\n" + "=" * 70)
    print("DUPLICATE CIRCULAR NUMBERS")
    print("=" * 70)

    if duplicate_numbers:
        for number, count in duplicate_numbers.items():
            print(f"{number} -> {count} records")
    else:
        print("No duplicate circular numbers found.")

    # ---------------------------------------------------------
    # 2. Duplicate display numbers
    # ---------------------------------------------------------

    display_numbers = [
        record.display_number
        for record in records
        if record.display_number
    ]

    display_counts = Counter(display_numbers)

    duplicate_display_numbers = {
        number: count
        for number, count in display_counts.items()
        if count > 1
    }

    print("\n" + "=" * 70)
    print("DUPLICATE DISPLAY NUMBERS")
    print("=" * 70)

    if duplicate_display_numbers:
        for number, count in duplicate_display_numbers.items():
            print(f"{number} -> {count} records")
    else:
        print("No duplicate display numbers found.")

    # ---------------------------------------------------------
    # 3. Duplicate file links
    # ---------------------------------------------------------

    file_links = [
        record.file_link
        for record in records
        if record.file_link
    ]

    link_counts = Counter(file_links)

    duplicate_links = {
        link: count
        for link, count in link_counts.items()
        if count > 1
    }

    print("\n" + "=" * 70)
    print("DUPLICATE FILE LINKS")
    print("=" * 70)

    if duplicate_links:
        for link, count in duplicate_links.items():
            print(f"{link} -> {count} records")
    else:
        print("No duplicate file links found.")

    # ---------------------------------------------------------
    # 4. Show complete duplicate records
    # ---------------------------------------------------------

    duplicate_numbers_set = set(duplicate_numbers)

    duplicate_records = [
        record
        for record in records
        if record.circular_number in duplicate_numbers_set
    ]

    print("\n" + "=" * 70)
    print("COMPLETE RECORDS WITH DUPLICATE CIRCULAR NUMBERS")
    print("=" * 70)

    if duplicate_records:
        for record in duplicate_records:
            print("\n----------------------------------------")
            print(f"Circular Number : {record.circular_number}")
            print(f"Display Number  : {record.display_number}")
            print(f"Date            : {record.circular_date}")
            print(f"Category        : {record.category}")
            print(f"Department      : {record.department}")
            print(f"Subject         : {record.subject}")
            print(f"File            : {record.filename}")
            print(f"Link            : {record.file_link}")
    else:
        print("No duplicate records to inspect.")


if __name__ == "__main__":
    asyncio.run(main())