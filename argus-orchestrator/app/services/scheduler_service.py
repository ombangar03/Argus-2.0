import logging
import json 
import time
from datetime import datetime

from app.platform.redis import redis_client
from app.platform.mongodb import scheduled_units
from app.core.config import settings

logger = logging.getLogger(__name__)

ACTIVE_KEYS_SET = "schedule:active_keys"



def _due_key(platform: str, priority: str) -> str:
    """
    Build the redis ZSET key for a given platform & priority
    """

    return f"scheduler:due:{platform.lower()}:{priority.lower()}"


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
) -> None:
    """
    Registers or update a unit in redis scheduling structures.
    """

    if cadence_seconds <= 0:
        raise ValueError("cadence_second must be greater than zero.")

    platform = platform.strip().lower()
    priority = priority.strip().lower()

    if not platform or not priority or not unit_id.strip():
        raise ValueError("unit_id, platforms, and priority are required.")

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
        pipe.zadd(
            due_key,
            {unit_id: _timestamp(next_run_at)},
        )

        pipe.hset(unit_key, mapping=metadata)
        pipe.sadd(ACTIVE_KEYS_SET, unit_key)

        await pipe.execute()


async def get_due_unit_id(
    platform: str,
    priority: str,
    limit: int = 100,
    now: float | None = None,
) -> list[str]:

    """Return unit IDs whose scheduled execution time has expired.
    this reads candidate only; it does not claim or remove them. 
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



_DISPATCH_DUE_UNIT_SCRIPT = """
local unit_id = ARGV[1]
local now = tonumber(ARGV[2])

-- Check whether the unit is scheduled.
local due_score = redis.call("ZSCORE", KEYS[1], unit_id)

if not due_score then
    return {0, "not_scheduled"}
end

-- Do not dispatch a unit before its due time.
if tonumber(due_score) > now then
    return {0, "not_due"}
end

-- Read the unit metadata.
local term = redis.call("HGET", KEYS[2], "term")
local platform = redis.call("HGET", KEYS[2], "platform")
local priority = redis.call("HGET", KEYS[2], "priority")
local org_id = redis.call("HGET", KEYS[2], "org_id") or ""
local search_options =
    redis.call("HGET", KEYS[2], "search_options") or "{}"
local cadence =
    tonumber(redis.call("HGET", KEYS[2], "cadence_seconds"))
local enabled = redis.call("HGET", KEYS[2], "enabled")

-- Remove disabled units from the due schedule.
if enabled == "false" then
    redis.call("ZREM", KEYS[1], unit_id)
    return {0, "disabled"}
end

-- Validate required metadata before writing anything.
if not term or not platform or not priority or not cadence
   or cadence <= 0 then
    return redis.error_reply("Invalid scheduled unit metadata")
end

local scheduled_for = tostring(due_score)
local request_id = "scheduled:" .. unit_id .. ":" .. scheduled_for
local next_run_at = now + cadence

-- Publish the job to the connector stream.
local stream_id = redis.call(
    "XADD", KEYS[3], "*",
    "request_id", request_id,
    "unit_id", unit_id,
    "term", term,
    "platform", platform,
    "priority", priority,
    "org_id", org_id,
    "search_options", search_options,
    "scheduled_for", scheduled_for,
    "dispatched_at", tostring(now),
    "cadence_seconds", tostring(cadence)
)

-- Move the next execution time forward.
redis.call("ZADD", KEYS[1], next_run_at, unit_id)

return {1, stream_id, tostring(next_run_at), request_id}
"""


async def dispatch_due_unit(
    unit_id: str,
    platform: str,
    priority: str,
    now: float | None = None,
) -> dict[str, str] | None:
    """Atomically publish one due unit and schedule its next run."""

    current_time = time.time() if now is None else float(now)

    platform = platform.strip().lower()
    priority = priority.strip().lower()

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)
    stream_name = f"queue:connector:{platform}:{priority}"

    result = await redis_client.eval(
        _DISPATCH_DUE_UNIT_SCRIPT,
        3,
        due_key,
        unit_key,
        stream_name,
        unit_id,
        current_time,
    )

    if int(result[0]) != 1:
        return None

    return {
        "stream_name": stream_name,
        "stream_message_id": str(result[1]),
        "next_run_at": str(result[2]),
        "request_id": str(result[3]),
    }


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
    

    for active_keys in sorted(active_keys):
        try:
            platforms, priority = active_keys.split(":", 1)
        
        except ValueError:
            logger.warning("Ignoring invalid scheduler key: %s", active_keys)
            continue
        
        #get candidate whose scheduled time has arrieved.
        due_unit_ids = await get_due_unit_id(
            platform=platforms,
            priority=priority,
            limit=limit_per_key,
            now=current_time,
        )

        # each unit is checked atomically before dispatch.

        for unit_id in due_unit_ids:
            result = await dispatch_due_unit(
                unit_id=unit_id,
                platform=platforms,
                priority=priority,
                now=current_time,
            )

            if result is None:
                continue

            dispatched_count += 1

            logger.info(
                "Dispatched unit %s to %s; next run at %s",
                unit_id,
                result["stream_name"],
                result["next_run_at"],
            )

        return dispatched_count
