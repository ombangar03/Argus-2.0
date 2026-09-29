from app.connectors.nse.connector import NSEConnector
from app.connectors.registry import get_connector


def main():

    connector = get_connector("nse_announcements")

    print("Connector type:", type(connector).__name__)
    print("Is NSEConnector:", isinstance(connector, NSEConnector))


if __name__ == "__main__":
    main()