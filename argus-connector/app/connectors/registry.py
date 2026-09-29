from app.connectors.rss.connector import RSSConnector
from app.connectors.nse.connector import NSEConnector


CONNECTORS = {
    "rss": RSSConnector(),
    "nse_announcements": NSEConnector()
}


def get_connector(source_type: str):
    connector = CONNECTORS.get(source_type)

    if not connector:
        raise ValueError(f"Unsupported connector: {source_type}")

    return connector