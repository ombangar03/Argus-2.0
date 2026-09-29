from app.connectors.nse.parser import parse_nse_rss


SAMPLE_NSE_XML = """
<rss version="2.0">
    <channel>
        <title>NSE Announcements</title>

        <item>
            <title>Supreme Facility Management Limited</title>
            <link>https://example.com/announcement.pdf</link>
            <description>
                AGM Proccedings and Voting Result |SUBJECT: Shareholders meeting
            </description>
            <pubDate>29-Sep-2026 09:59:57</pubDate>
        </item>

        <item>
            <title>Ajooni Biotech Limited</title>
            <link>https://example.com/trading-window.pdf</link>
            <description>
                Trading Window |SUBJECT: Trading Window
            </description>
            <pubDate>29-Sep-2026 09:59:52</pubDate>
        </item>

        <item>
            <title>Example NAV Announcement</title>
            <link></link>
            <description>
                Declaration of NAV
            </description>
            <pubDate>29-Sep-2026 09:59:00</pubDate>
        </item>

    </channel>
</rss>
"""


def main():
    request_id = "test_nse_parser_001"

    announcements = parse_nse_rss(
        xml_content=SAMPLE_NSE_XML,
        request_id=request_id,
    )

    print(f"Total announcements: {len(announcements)}")
    print()

    for index, announcement in enumerate(announcements, start=1):
        print(f"Announcement #{index}")
        print("Request ID:", announcement.request_id)
        print("Source:", announcement.source)
        print("Source Type:", announcement.source_type)
        print("Title:", announcement.title)
        print("Link:", announcement.link)
        print("Description:", announcement.description)
        print("Subject:", announcement.subject)
        print("Published At:", announcement.published_at)
        print("Created At:", announcement.created_at)
        print("-" * 60)


if __name__ == "__main__":
    main()