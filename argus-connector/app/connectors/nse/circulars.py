import logging
from datetime import datetime, timezone

import httpx

from app.connectors.nse.models import NSECirculars

logger = logging.getLogger(__name__)

NSE_CIRCULAR_API_URL = (
    "https://www.nseindia.com/api/circulars"
)


NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/154.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}

async def fetch_nse_circulars(
    request_id: str,
    from_date: str,
    to_date: str,
) -> list[NSECirculars]:

    params = {
        "fromDate": from_date,
        "toDate": to_date,
    }

    async with httpx.AsyncClient(
        headers=NSE_HEADERS,
        timeout=30.0,
        follow_redirects=True,
    ) as client:

        response = await client.get(
            NSE_CIRCULAR_API_URL,
            params=params,
        )

        response.raise_for_status()

        logger.info(
            "NSE Circulars API Fetched successfully."
            "Status: %s, from=%s, to=%s",
            response.status_code,
            from_date,
            to_date,
        )

        payload = response.json()
        
        # print("\nPAYLOAD TYPE:", type(payload))

        # print("\nPAYLOAD KEYS:")
        # if isinstance(payload, dict):
        #     print(payload.keys())

        # print("\nFIRST RECORD:")
        # if isinstance(payload, dict):
        #     data = payload.get("data", [])
        #     if data:
        #         print(data[0])
        #     else:
        #         print("NO RECORDS FOUND")

    circulars = []

    for record in payload.get("data", []):
        circular = _parse_circular(
            record=record,
            request_id=request_id,
        )

        circulars.append(circular)
        
    logger.info(
        "NSE Circulars parsed successfully. Circulars: %d",
        len(circulars),
    )

    return circulars


def _parse_circular(record: dict, request_id: str) -> NSECirculars:

    # circular_date = _parse_circular_date(
    #     record.get("circularDate")
    # )

    return NSECirculars(
        request_id=request_id,
        source="nse",
        source_type="nse_circular",
        circular_date=_parse_circular_date(
            record.get("cirDate")
        ),
        circular_display_date=record.get("cirDisplayDate"),
        category=record.get("circCategory"),
        company=record.get("circCompany"),
        department=record.get("circDepartment"),
        display_number=record.get("circDisplayNo"),
        circular_number=record.get("circNumber"),
        subject=record.get("sub"),
        file_link=record.get("circFilelink"),
        file_size=record.get("circFileSize"),
        filename=record.get("circFilename"),
        file_department=record.get("fileDept"),
        file_extension=record.get("fileExt"),
    )


def _parse_circular_date(
    value: str | None,
) -> datetime | None:

    if not value:
        return None
    
    try: 
        return datetime.strptime(
            value,
            "%Y%m%d", 
        ).replace(tzinfo=timezone.utc)
    
    except ValueError as exc:
        logger.error(
            "Failed to parse circular date '%s': %s",
            value,
            exc,
        )
        return None



        