import logging
import json
import time
from datetime import datetime, timezone

from app.services.scheduled_request_service import (
    create_scheduled_rss_request,
)

from app.platform.redis import redis_client
from app.platform.mongodb import scheduled_units
from app.core.config import settings

logger = logging.getLogger(__name__)

ACTIVE_KEYS_SET = "schedule:active_keys"
MANAGED_UNITS_SET = "schedule:managed_unit_ids"


def _due_key(platform: str, priority: str) -> str:
    """
    Build the redis ZSET key for a given platform & priority
    """

    return f"schedule:due:{platform.lower()}:{priority.lower()}"


def _unit_key(unit_id: str) -> str:
    """
    Build the redis HASH key for a scheduler unit.
    """
    return f"unit:{unit_id}"


def _timestamp(value: datetime | int | float) -> float:
    """
    Convert a datetime or Unix timestamp into seconds.
    """

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.timestamp()

    return float(value)



async def register_scheduled_unit(
    unit_id: str,
    platform: str,
    priority: str,
    next_run_at: datetime | int | float,
    term: str,
    cadence_seconds: int,
    org_id: str = "",
    search_options: dict | None = None,
    enabled: bool = True,
    managed_by_mongodb: bool = False,
    preserve_due_time: bool = False,
) -> None:
    """
    Registers or update a unit in redis scheduling structures.

    When preserve_due_time=True, initialize the due time only if
    this unit does not already exist in the target sorted set.
    """

    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds <= 0
    ):
        raise ValueError("cadence_second must be a positive integer.")

    if not isinstance(unit_id, str) or not unit_id.strip():
        raise ValueError("unit_id is required.")

    if not isinstance(platform, str) or not platform.strip():
        raise ValueError("platform is required.")

    if not isinstance(priority, str) or not priority.strip():
        raise ValueError("priority is required.")


    platform = platform.strip().lower()
    priority = priority.strip().lower()
    unit_id = unit_id.strip().lower()

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    metadata = {
        "term": term,
        "org_id": org_id,
        "search_options": json.dumps(search_options or {}),
        "cadence_seconds": str(cadence_seconds),
        "platform": platform,
        "priority": priority,
        "enabled": str(enabled).lower(),
    }

    async with redis_client.pipeline(transaction=True) as pipe:
        if preserve_due_time:
            #existing due score remain unchanged.
            #new units receive their initial score
            pipe.zadd(
                due_key,
                {unit_id: _timestamp(next_run_at)},
                nx=True
            )
        else:
            # preserve the existing behavior for other callers
            pipe.zadd(
                due_key,
                {unit_id: _timestamp(next_run_at)}
            )

        pipe.hset(unit_key, mapping=metadata)
        pipe.sadd(ACTIVE_KEYS_SET,  f"{platform}:{priority}")

        if managed_by_mongodb:
            pipe.sadd(MANAGED_UNITS_SET, unit_id)

        await pipe.execute()


async def sync_scheduled_units_from_mongodb() -> int:
    """
    Synchronized Mongodb scheduler definitions into Redis.

    Mongodb is the source of truth for managed schedules.
    Redis Stores the due-time index and cached metadata.

    returns the number of valid enabled units synchronized.
    """

    # read mongodb  first, if this fails, do not modify redis
    cursor = scheduled_units.find({})
    documents = await cursor.to_list(length=None)

    synchronized_ids: set[str] = set()
    seen_ids: set[str] = set()
    disabled_ids: set[str] = set()

    synchronized_count = 0

    for document in documents:
        unit_id = document.get("unit_id") or document.get("_id")

        # ignore malformed documents without a usable ID.
        if not isinstance(unit_id, str) or not unit_id.strip():
            logger.warning(
                "Skipping scheduled unit with missing or invalid unit_id."
            )

            continue

        unit_id = unit_id.strip()
        seen_ids.add(unit_id)

        # disabled definitions should not remain scheduled
        enabled = document.get("enabled")

        if not isinstance(enabled, bool):
            logger.warning(
                "Skipping scheduled unit %s: enabled must be a boolean.",
                unit_id,
            )
            continue

        if not enabled:
            disabled_ids.add(unit_id)
            continue

        try:
            platform = document["platform"]
            priority = document["priority"]
            term = document["term"]
            next_run_at = document["next_run_at"]
            cadence_seconds = document["cadence_seconds"]

            if not isinstance(platform, str) or not platform.strip():
                raise ValueError("platform must be a non-empty string")

            if not isinstance(priority, str) or not priority.strip():
                raise ValueError("priority must be a non-empty string")

            if not isinstance(term, str) or not term.strip():
                raise ValueError("term must be a non-empty string")

            if isinstance(cadence_seconds, bool) or not isinstance(
                cadence_seconds, int
            ):
                raise ValueError("cadence_seconds must be an integer.")


            if cadence_seconds <= 0:
                raise ValueError("cadence_seconds must be positive")

            if not isinstance(next_run_at, (datetime, int, float)):
                raise ValueError(
                    "next_run_at must be a datetime or Unix timestamp"
                )

            await register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=next_run_at,
                term=term.strip(),
                cadence_seconds=cadence_seconds,
                org_id=str(document.get("org_id", "")),
                search_options=document.get("search_options") or {},
                enabled=True,
                managed_by_mongodb=True,
                preserve_due_time=True,
            )

            synchronized_ids.add(unit_id)
            synchronized_count += 1

        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Skipping invalid scheduled unit %s: %s",
                unit_id,
                exc,
            )

    # reconcile stale mongo-managed units only after successful read
    existing_managed_ids = await redis_client.smembers(MANAGED_UNITS_SET)

    stale_ids = (
        existing_managed_ids - seen_ids
    ) | disabled_ids

    for stale_id in stale_ids   :
        unit_key = _unit_key(stale_id)

        metadata = await redis_client.hgetall(unit_key)
        platform = metadata.get("platform")
        priority = metadata.get("priority")

        async with redis_client.pipeline(transaction=True) as pipe:
            if platform and priority:
                due_key = _due_key(platform, priority)
                pipe.zrem(due_key, stale_id)

            pipe.delete(unit_key)
            pipe.srem(MANAGED_UNITS_SET, stale_id)
            await pipe.execute()

        logger.info(
            "Removed stale Mongo-managed schedule: %s", stale_id
        )

    logger.info(
        "MongoDB schedule synchronization complete: %s unit(s) synchronized.",
        synchronized_count,
    )

    return synchronized_count


_ADVANCE_DUE_TIME_SCRIPT = """
local unit_id = ARGV[1]
local expected_due_time = tonumber(ARGV[2])
local next_run_at = tonumber(ARGV[3])

local current_due_time = redis.call("ZSCORE", KEYS[1], unit_id)

if not current_due_time then
    return 0
end

if tonumber(current_due_time) ~= expected_due_time then
    return 0
end

redis.call("ZADD", KEYS[1], next_run_at, unit_id)

return 1
"""


async def dispatch_due_unit(
    unit_id: str,
    platform: str,
    priority: str,
    now: float | None = None,
) -> dict[str, str] | None:
    """
    Create a durable MongoDB request for one due schedule,
    then advance its Redis due time.

    Step A: write the request to MongoDB (idempotent via deterministic ID).
    Step B: advance the Redis due score only after the request exists.
    If another dispatcher already advanced this unit, the compare-and-swap
    script returns 0 and we leave their newer score untouched.
    """

    current_time = time.time() if now is None else float(now)

    platform = platform.strip().lower()
    priority = priority.strip().lower()

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    # Read the current due score.
    due_score = await redis_client.zscore(due_key, unit_id)

    if due_score is None:
        return None

    scheduled_for = float(due_score)

    if scheduled_for > current_time:
        return None

    # Read the schedule metadata.
    metadata = await redis_client.hgetall(unit_key)

    if not metadata:
        logger.warning(
            "Missing metadata for scheduled unit %s",
            unit_id,
        )
        return None

    if metadata.get("enabled") == "false":
        await redis_client.zrem(due_key, unit_id)
        return None

    term = metadata.get("term")
    stored_platform = metadata.get("platform")
    stored_priority = metadata.get("priority")
    cadence_value = metadata.get("cadence_seconds")

    if not term or not stored_platform or not stored_priority:
        raise ValueError(
            f"Invalid metadata for scheduled unit {unit_id}"
        )

    if stored_platform != platform or stored_priority != priority:
        logger.warning(
            "Schedule metadata does not match its Redis index: %s",
            unit_id,
        )
        return None

    try:
        cadence_seconds = int(cadence_value)
        search_options = json.loads(
            metadata.get("search_options") or "{}"
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Invalid metadata for scheduled unit {unit_id}"
        ) from exc

    if cadence_seconds <= 0:
        raise ValueError(
            f"Invalid cadence for scheduled unit {unit_id}"
        )

    if not isinstance(search_options, dict):
        raise ValueError(
            f"search_options must be an object for unit {unit_id}"
        )

    # Step A: create the durable request first.
    request = await create_scheduled_rss_request(
        unit_id=unit_id,
        term=term,
        scheduled_for=scheduled_for,
        priority=stored_priority,
        org_id=metadata.get("org_id", ""),
        search_options=search_options,
    )

    # Step B: advance Redis only after the request exists
    # or the request service confirms that it already exists.
    next_run_at = current_time + cadence_seconds

    advanced = await redis_client.eval(
        _ADVANCE_DUE_TIME_SCRIPT,
        1,
        due_key,
        unit_id,
        scheduled_for,
        next_run_at,
    )

    if int(advanced) != 1:
        logger.info(
            "Schedule %s was already advanced or changed; "
            "request %s remains durable",
            unit_id,
            request["request_id"],
        )
        return None

    return {
        "request_id": request["request_id"],
        "next_run_at": str(next_run_at),
        "created": str(request["created"]),
    }


async def get_due_unit_id(
    platform: str,
    priority: str,
    limit: int = 100,
    now: float | None = None,
) -> list[str]:
    """Return unit IDs whose scheduled execution time has expired.
    This reads candidates only; it does not claim or remove them.
    """

    if limit <= 0:
        raise ValueError("Limit must be greater than zero")

    due_key = _due_key(platform, priority)
    current_time = time.time() if now is None else now

    return await redis_client.zrangebyscore(
        due_key,
        min="-inf",
        max=current_time,
        start=0,
        num=limit,
    )


async def dispatch_due_units_once(
    limit_per_key: int = 100,
    now: float | None = None,
    platform_priority_keys: set[str] | None = None,
) -> int:
    """Dispatch due units across selected or all active combinations."""

    if limit_per_key <= 0:
        raise ValueError("limit_per_key must be greater than zero")

    current_time = time.time() if now is None else float(now)
    dispatched_count = 0

    if platform_priority_keys is None:
        active_keys = await redis_client.smembers(ACTIVE_KEYS_SET)
    else:
        active_keys = platform_priority_keys

    for active_key in sorted(active_keys):
        try:
            platform, priority = active_key.split(":", 1)
        except ValueError:
            logger.warning("Ignoring invalid scheduler key: %s", active_key)
            continue

        # Find units whose scheduled time has arrived.
        due_unit_ids = await get_due_unit_id(
            platform=platform,
            priority=priority,
            limit=limit_per_key,
            now=current_time,
        )

        # Create a durable MongoDB request for each due unit and advance its schedule.
        for unit_id in due_unit_ids:
            result = await dispatch_due_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                now=current_time,
            )

            if result is None:
                continue

            dispatched_count += 1

            logger.info(
                "Created scheduled request %s for unit %s; next run at %s",
                result["request_id"],
                unit_id,
                result["next_run_at"],
            )

    return dispatched_count
