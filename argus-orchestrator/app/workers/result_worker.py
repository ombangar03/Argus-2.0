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

    def __init__(self, block_ms: int = 2000, batch_size: int = 10):
        self.stream_name = settings.result_stream
        self.group_name = settings.consumer_group
        self.consumer_name = settings.consumer_name
        self.block_ms = block_ms
        self.batch_size = batch_size
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
            logger.info(f"Created consumer group '{self.group_name}' on stream '{self.stream_name}'.")
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(f"Consumer group '{self.group_name}' already exists.")
            else:
                logger.error(f"Failed to create consumer group: {exc}")
                raise

    def _parse_message_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses Redis stream message fields. If articles were serialized as a JSON string,
        decodes them into standard Python data structures.
        """
        parsed = dict(raw_data)

        # If connector serialized full payload as JSON string
        if "payload" in parsed and isinstance(parsed["payload"], str):
            try:
                return json.loads(parsed["payload"])
            except json.JSONDecodeError:
                pass

        # If connector serialized 'items' field as JSON string
        if "items" in parsed and isinstance(parsed["items"], str):
            try:
                parsed["items"] = json.loads(parsed["items"])
            except json.JSONDecodeError:
                pass

        return parsed

    async def start(self):
        """
        Starts the continuous consumer worker loop.
        """
        self._is_running = True  # worker =  running
        self._stop_event.clear() # stop signal = OFF.

        logger.info("Initializing Result Worker...")

        # 1. Pre-flight health checks
        mongo_ok = await check_mongodb()  # is mongodb healthy?
        redis_ok = await check_redis()    # is redis healthy?

        if not mongo_ok or not redis_ok:
            logger.error("Database or Redis is not healthy. Result Worker cannot start.")
            return

        # 2. Ensure MongoDB indexes exist
        await ensure_indexes()

        # 3. Ensure Redis consumer group exists
        await self._setup_consumer_group()

        logger.info(
            f"Result Worker listening on stream '{self.stream_name}' "
            f"[Group: {self.group_name}, Consumer: {self.consumer_name}]"
        )

        # 4. Main consumer loop
        while self._is_running and not self._stop_event.is_set():
            try:
                # Read new messages from Redis Stream
                entries = await redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms
                )

                if not entries:
                    # No new messages in this block window; loop again
                    continue

                for stream_key, messages in entries:
                    for message_id, raw_fields in messages:
                        try:
                            # Parse payload and pass to result_service
                            payload = self._parse_message_data(raw_fields)
                            summary = await process_crawl_result(payload)

                            logger.info(
                                f"Processed result message {message_id}: "
                                f"request_id={summary.get('request_id')} "
                                f"status={summary.get('status')} "
                                f"saved={summary.get('items_inserted', 0)}"
                            )

                            # Acknowledge message so Redis removes it from pending
                            await redis_client.xack(self.stream_name, self.group_name, message_id)

                        except Exception as item_exc:
                            logger.exception(
                                f"Failed to process result message {message_id}: {item_exc}"
                            )  # redis will still consider the message is pending for the consumer group

            except asyncio.CancelledError:
                logger.info("Result Worker received cancellation signal.")
                break

            except Exception as exc:
                logger.exception(f"Unexpected error in Result Worker loop: {exc}")
                await asyncio.sleep(5.0)   # wait for 5 sec and try again.

        logger.info("Result Worker loop has exited.")

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
