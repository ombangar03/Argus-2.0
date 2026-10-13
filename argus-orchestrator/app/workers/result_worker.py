"""
Inbound Result Worker Daemon (The "Inbound Ingestor")

Continuously listens to Redis Stream 'connector:rss:results' via consumer group
'orchestrator-results', ingests completed crawl results, saves articles into MongoDB
without duplicates, and acknowledges messages via xack.
"""

import asyncio
import json
import logging
import signal
from typing import Any, Dict

import redis.exceptions

from app.core.config import settings
from app.core.logging import setup_logging
from app.platform.mongodb import check_mongodb
from app.platform.redis import check_redis, redis_client
from app.services.result_service import ensure_indexes, process_crawl_result

logger = logging.getLogger(__name__)


class ResultWorker:
    """
    Continuous background consumer that reads incoming crawl results from Redis
    and invokes result_service to persist articles and update request statuses.
    """
    def __init__(
        self,
        block_ms: int = 2000,
        batch_size: int = 10,
        claim_idle_ms: int | None = None,
        recovery_interval: int | None = None,
    ):
        self.stream_name = settings.result_stream
        self.group_name = settings.consumer_group
        self.consumer_name = settings.consumer_name
        self.recovery_consumer_name = f"{self.consumer_name}-recovery"
        self.block_ms = block_ms
        self.batch_size = batch_size

        self.claim_idle_ms = (
            settings.result_claim_idle_ms
            if claim_idle_ms is None
            else claim_idle_ms
        )

        self.recovery_interval = (
            settings.result_recovery_interval
            if recovery_interval is None
            else recovery_interval
        )
        self._is_running = False
        self._stop_event = asyncio.Event()

    async def _setup_consumer_group(self):
        """
        Ensures the Redis Stream consumer group exists.
        Creates both the stream and the group if they don't already exist.
        """
        try:
            await redis_client.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id="0",
                mkstream=True
            )
            logger.info(
                f"Created consumer group '{self.group_name}' on stream '{self.stream_name}'."
            )

        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    f"Consumer group '{self.group_name}' already exists."
                )
            else:
                logger.exception(f"Failed to create consumer group: {exc}")
                raise

    def _parse_message_data(
        self, raw_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Parses Redis stream message fields. If articles were serialized as a JSON string,
        decodes them into standard Python data structures.
        """
        parsed = dict(raw_data)

        # If connector serialized full payload as JSON string
        if "payload" in parsed and isinstance(parsed["payload"], str):
            try:
                payload = json.loads(parsed["payload"])

                if isinstance(payload, dict):
                    return payload
                raise ValueError("Redis payload must be decoded to a JSON object.")
            except json.JSONDecodeError:
                logger.exception(f"Invalid JSON in Redis Payload")
                raise

        # If connector serialized 'items' field as JSON string
        if "items" in parsed and isinstance(parsed["items"], str):
            try:
                parsed["items"] = json.loads(parsed["items"])
            except json.JSONDecodeError:
                logger.exception("Invalid JSON in Redis items field.")
                raise

        return parsed
    # ---------------------------------------------------------------------------------------------------
    async def _process_message(self, message_id, raw_fields):
        """
        Process a result and acknowledge it only after successful processing.
        Failed message remain pending for recovery.
        """
        payload = self._parse_message_data(raw_fields)
        summary = await process_crawl_result(payload)

        logger.info(
            "Process result message %s: request_id=%s status=%s, saved=%s",
            message_id,
            summary.get("request_id"),
            summary.get("status"),
            summary.get("items_inserted", 0),
        )

        await redis_client.xack(
            self.stream_name,
            self.group_name,
            message_id,
        )

    # ---------------------------------------------------------------------------------
    async def _consumer_new_messages(self):
        """
        Consume message that have not yet been delivered to this group.
        """
        logger.info(
            "New result-message consumer loop started."
        )

        while self._is_running and not self._stop_event.is_set():
            try:
                entries = await redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )

                for _, messages in entries or []:
                    for message_id, raw_fields in messages:
                        try:
                            await self._process_message(
                                message_id, raw_fields
                            )
                        except Exception:
                            logger.exception(
                                "Failed to process result message %s.",
                                message_id
                            )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Unexpected error in new result-message loop."
                )
                await asyncio.sleep(5)

        logger.info("New result-message consumer loop exited.")

    # -------------------------------------------------------------------------
    async def _recover_pending_messages (self):
        """
        claim and retry stale pending message using XAUTOCLAIM.
        """
        logger.info(
            "Result recovery loop started (idel threshold=%sms).",
            self.claim_idle_ms,
        )

        claim_start_id = "0-0"

        while self._is_running and not self._stop_event.is_set():
            try:
                result = await redis_client.xautoclaim(
                    name=self.stream_name,
                    groupname=self.group_name,
                    consumername=self.recovery_consumer_name,
                    min_idle_time=self.claim_idle_ms,
                    start_id=claim_start_id,
                    count=self.batch_size,
                )

                next_start_id, messages, deleted_ids = result
                claim_start_id = next_start_id

                if deleted_ids:
                    logger.warning(
                        "Redis reported %s deleted pending result IDs.",
                        len(deleted_ids)
                    )

                for message_id, raw_fields in messages:
                    try:
                        logger.info(
                            "Processing reclaimed result message %s.",
                            message_id
                        )
                        await self._process_message(message_id, raw_fields)
                    except Exception:
                        logger.exception(
                            "Failed to process reclaimed result %s.",
                            message_id,
                        )
                #Redis returns 0-0 when the current scan reaches its end.
                # Pause before starting the next recovery scan
                if claim_start_id == "0-0":
                    await asyncio.sleep(self.recovery_interval)
            except asyncio.CancelledError:
                raise
            except redis.exception.ResponseError as exc:
                logger.exception(
                    "Redis error during result XAUTOCLAIM: %s", exc
                )

                await asyncio.sleep(5)
        logger.info("Result recovery loop exited")

    # -------------------------------------------------------------------------
    async def start(self):
        """
        Run health-checks initialize dependencies, and start both loops.
        """
        self._is_running = True  # worker =  running
        self._stop_event.clear() # stop signal = OFF.

        logger.info("Initializing Result Worker...")

        try:
            # 1. Pre-flight health checks
            mongo_ok = await check_mongodb()  # is mongodb healthy?
            redis_ok = await check_redis()    # is redis healthy?

            if not mongo_ok or not redis_ok:
                logger.error(
                    "Database or Redis is not healthy. Result Worker cannot start."
                )
                return

            # 2. Ensure MongoDB indexes exist
            await ensure_indexes()
            # 3. Ensure Redis consumer group exists
            await self._setup_consumer_group()

            logger.info(
                "Result Worker listening on '%s' "
                    "[group=%s consumer=%s recovery_consumer=%s].",
                    self.stream_name,
                    self.group_name,
                    self.consumer_name,
                    self.recovery_consumer_name,
            )

            await asyncio.gather(
                self._consumer_new_messages(),
                self._recover_pending_messages(),
            )
        finally:
            self._is_running = False
            logger.info("Result Worker loop has exited")
        # 4. Main consumer loop
        # while self._is_running and not self._stop_event.is_set():
        #     try:
        #         # Read new messages from Redis Stream
        #         entries = await redis_client.xreadgroup(
        #             groupname=self.group_name,
        #             consumername=self.consumer_name,
        #             streams={self.stream_name: ">"},
        #             count=self.batch_size,
        #             block=self.block_ms
        #         )

        #         if not entries:
        #             # No new messages in this block window; loop again
        #             continue

        #         for stream_key, messages in entries:
        #             for message_id, raw_fields in messages:
        #                 try:
        #                     # Parse payload and pass to result_service
        #                     payload = self._parse_message_data(raw_fields)
        #                     summary = await process_crawl_result(payload)

        #                     logger.info(
        #                         f"Processed result message {message_id}: "
        #                         f"request_id={summary.get('request_id')} "
        #                         f"status={summary.get('status')} "
        #                         f"saved={summary.get('items_inserted', 0)}"
        #                     )

        #                     # Acknowledge message so Redis removes it from pending
        #                     await redis_client.xack(self.stream_name, self.group_name, message_id)

        #                 except Exception as item_exc:
        #                     logger.exception(
        #                         f"Failed to process result message {message_id}: {item_exc}"
        #                     )  # redis will still consider the message is pending for the consumer group

        #     except asyncio.CancelledError:
        #         logger.info("Result Worker received cancellation signal.")
        #         break

        #     except Exception as exc:
        #         logger.exception(f"Unexpected error in Result Worker loop: {exc}")
        #         await asyncio.sleep(5.0)   # wait for 5 sec and try again.

        # logger.info("Result Worker loop has exited.")

    def stop(self):
        """
        Gracefully stops the worker loop.
        """
        logger.info("Stopping Result Worker gracefully...")
        self._is_running = False
        self._stop_event.set()


async def run_worker():
    """
    Entrypoint for running the worker daemon with OS signal handling.
    """
    setup_logging()
    worker = ResultWorker(block_ms=2000, batch_size=10)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except (NotImplementedError, AttributeError):
            pass

    try:
        await worker.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.stop()
        logger.info("Result Worker terminated.")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        print("\nResult Worker terminated by user.")
