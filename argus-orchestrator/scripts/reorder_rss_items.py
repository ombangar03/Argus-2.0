"""
Migration script: re-orders existing rss_items documents to match the RSSItem schema field order.
Run once from the argus-orchestrator directory:
    python scripts/reorder_rss_items.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.platform.mongodb import rss_items


FIELD_ORDER = [
    "item_hash",
    "request_id",
    "source",
    "source_type",
    "title",
    "link",
    "description",
    "summary",
    "author",
    "published_at",
    "created_at",
]


def reorder_doc(doc: dict) -> dict:
    """Returns a new dict with fields in the canonical FIELD_ORDER, extras appended at the end."""
    ordered = {"_id": doc["_id"]}  # _id always first
    for field in FIELD_ORDER:
        if field in doc:
            ordered[field] = doc[field]
    # Append any extra fields not in FIELD_ORDER (future-proof)
    for key, val in doc.items():
        if key not in ordered:
            ordered[key] = val
    return ordered


async def migrate():
    total = await rss_items.count_documents({})
    print(f"Found {total} documents in rss_items. Reordering...")

    updated = 0
    skipped = 0

    async for doc in rss_items.find({}):
        original_keys = [k for k in doc.keys() if k != "_id"]
        expected_keys = [f for f in FIELD_ORDER if f in doc]

        # Check if fields are already in the right order
        if original_keys[:len(expected_keys)] == expected_keys:
            skipped += 1
            continue

        ordered = reorder_doc(doc)

        # MongoDB does not support reordering in-place, so we replace the document
        await rss_items.replace_one({"_id": doc["_id"]}, ordered)
        updated += 1

    print(f"Done. Updated: {updated}, Already correct: {skipped}")


if __name__ == "__main__":
    asyncio.run(migrate())
