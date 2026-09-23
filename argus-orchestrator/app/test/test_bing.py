import sys
from pathlib import Path

# Add project root directory to sys.path so 'app' package can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import feedparser
import requests
from app.services.bing_service import build_bing_rss_url

url = build_bing_rss_url("business news")

response = requests.get(
    url,
    headers = {"User-Agent": "Mozilla/5.0"},
    timeout = 20,
)

print("Status:", response.status_code)

feed = feedparser.parse(response.text)

print("Articles:", len(feed.entries))

for entry in feed.entries:
    print("\nTitle:", entry.get("title"))
    print("Link:", entry.get("link"))
    print("Published:", entry.get("published"))

