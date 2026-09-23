from urllib.parse import quote_plus

def build_bing_rss_url(query: str) -> str:
    encoded_query = quote_plus(query.strip())

    return (
        "https://www.bing.com/search?q="
        f"{encoded_query}&format=rss"
    )




