from app.connectors.rss.connector import RSSConnector


CONNECTORS = {
    "rss": RSSConnector(),
}


def get_connector(platform: str):
    connector = CONNECTORS.get(platform)

    if not connector:
        raise ValueError(f"Unsupported connector: {platform}")

    return connector