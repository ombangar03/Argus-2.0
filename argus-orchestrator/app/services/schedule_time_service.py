
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def calculate_next_run(
    now: datetime,
    run_times: list[str],
    timezone_name: str = "Asia/Kolkata",
) -> datetime:
    """
    Calculate the next scheduled clock time strictly after `now`.

    Example:
        09:45 IST -> 10:00 IST
        10:05 IST -> 19:00 IST
        19:05 IST -> next day at 10:00 IST
    """
    if not run_times:
        raise ValueError("run_times must not be empty")

    try:
        tz = ZoneInfo(timezone_name)
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"Invalid timezone: {timezone_name}"
        ) from exc

    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    local_now = now.astimezone(tz)
    parsed_times = []

    for value in run_times:
        try:
            parsed = time.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid run time: {value}"
            ) from exc

        if parsed.tzinfo is not None:
            raise ValueError(
                "run_times must use local clock times without a timezone"
            )

        parsed_times.append(parsed)

    if len(set(parsed_times)) != len(parsed_times):
        raise ValueError("run_times must not contain duplicates")

    parsed_times.sort()

    for run_time in parsed_times:
        candidate = datetime.combine(
            local_now.date(),
            run_time,
            tzinfo=tz,
        )

        if candidate > local_now:
            return candidate.astimezone(timezone.utc)

    next_day = local_now.date() + timedelta(days=1)

    candidate = datetime.combine(
        next_day,
        parsed_times[0],
        tzinfo=tz,
    )

    return candidate.astimezone(timezone.utc)
