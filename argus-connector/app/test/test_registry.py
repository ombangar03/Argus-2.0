from app.connectors.nse.connector import NSEConnector
from app.connectors.registry import get_connector


def main():

    connector = get_connector("nse_announcements")
    print("nse_announcements Connector type:", type(connector).__name__)
    print("Is NSEConnector:", isinstance(connector, NSEConnector))

    circular_connector = get_connector("nse_circular")
    print("nse_circular Connector type:", type(circular_connector).__name__)
    print("Is NSEConnector:", isinstance(circular_connector, NSEConnector))


if __name__ == "__main__":
    main()