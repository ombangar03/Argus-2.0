import asyncio
import json
import logging
import signal
from typing import Any, Dict


import redis.exceptions

from app.connectors.registry import get_connector
from app.core.config import settings
from app.core.logging import setup_logging
from app.platform.redis import check_redis, redis_client
from app.schemas.connector import ConnectorRequest

logger = logging.getLogger(__name__)


class ConnectorWorker:
    """
    Continuous background worker that pulls connector tasks from Redis Stream,
    executes the appropriate connector, publishes result to the result stream and acknowledge 
    the task via XACK.
    """

    def __init__(self, block_ms: int = 2000, batch_size: int = 5):
        self.stream_name = settings.connector_stream
        self.result_stream = settings.result_stream
        self.group_name = settings.consumer_group
        self.consumer_name = settings.consumer_name
        self.block_ms = block_ms
        self.batch_size = batch_size
        self._is_running = False
        self._stop_event = asyncio.Event()

    async def _setup_consumer_group(self):
        """Creates the Redis Stream and consumer group if they do not already exist."""
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
                # Normal: Group already exists from previous run
                logger.debug(f"Consumer group '{self.group_name}' already exists.")
            else:
                logger.error(f"Failed to create consumer group: {exc}")
                raise

    async def start(self):
        """Starts the continuous consumer worker loop."""
        self._is_running = True
        self._stop_event.clear()

        logger.info("Initializing Connector Worker....")

        # 1. Pre-flight health check
        redis_ok = await check_redis()
        if not redis_ok:
            logger.error("Cannot start worker - Redis is not reachable. Fix connection and restart.")
            return

        # 2. Ensure consumer group exists
        await self._setup_consumer_group()

        logger.info(
            f"Connector Worker listening on stream '{self.stream_name}' "
            f"[Group: {self.group_name}, Consumer: {self.consumer_name}]"
        )

        # 3. Continuous reading loop
        while self._is_running and not self._stop_event.is_set():
            try:
                entries = await redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms
                )

                if not entries:
                    continue

                for stream_key, messages in entries:
                    for message_id, raw_fields in messages:
                        await self._process_task(message_id, raw_fields)

            except asyncio.CancelledError:
                logger.info("Connector Worker received cancellation signal.")
                break

            except Exception as exc:
                logger.exception(f"Unexpected error in connector worker loop: {exc}")
                await asyncio.sleep(5.0)

        logger.info("Connector Worker loop has exited.")


    async def _process_task(self, message_id: str, fields: Dict[str, Any]):
        """Processes a single task, scrapes the feed, and acknowledges Redis."""
        request_id = fields.get("request_id")
        source = fields.get("source")
        source_type = fields.get("source_type")
        source_url = fields.get("source_url")
        metadata_raw = fields.get("metadata", {})

        try:
            metadata = json.loads(metadata_raw)
        
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                f"Invalid metadata for message {message_id}."
                f"Using empty metadata."
            )
            metadata = {}
        
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse metadata: {metadata_raw}")
            await redis_client.xack(
                self.stream_name, 
                self.group_name, 
                message_id
            )
            return

        if not request_id or not source or not source_type or not source_url:
            logger.error(
                f"Message {message_id} is missing"
                f"request_id, source, source_type or source_url: {fields}."
            )
            await redis_client.xack(
                self.stream_name, 
                self.group_name, 
                message_id
            )
            return

        logger.info(
            f"Processing task {message_id} ->"
            f"request_id={request_id}," 
            f"source={source}," 
            f"source_type={source_type},"
            f"source_url={source_url}")

        try:
            request = ConnectorRequest(
                request_id = request_id,
                source= source,
                source_type= source_type,
                source_url= source_url,
                metadata= metadata,
            )
            connector = get_connector(request.source_type)

            crawl_result = await connector.fetch(request)

            payload_json = crawl_result.model_dump_json()

            result_msg_id = await redis_client.xadd(
                self.result_stream, 
                {"payload": payload_json}
            )

            logger.info(
                f"Published result for {request_id} to "
                f"'{self.result_stream}' "
                f"(msg_id: {result_msg_id}, "
                f"items: {crawl_result.items_count})"
            )

            await redis_client.xack(
                self.stream_name, 
                self.group_name, 
                message_id
            )

        except Exception as exc:
            logger.exception(f"Failed to process task {message_id}: {exc}")


    def stop(self):
        """Gracefully signals the worker loop to stop."""
        logger.info("Stopping Connector Worker gracefully....")
        self._is_running = False
        self._stop_event.set()


async def run_worker():
    """Entrypoint to setup logging, start worker, and handle graceful shutdown."""
    setup_logging()
    worker = ConnectorWorker(block_ms=2000, batch_size=5)

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
        logger.info("Connector worker terminated.")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        print("\Connector Worker terminated by user.")
