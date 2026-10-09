from asyncio import timeout
from pydantic.deprecated.copy_internals import _calculate_keys
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
    Continuous background worker that:
    1. Reads new connector tasks from Redis Stream.
    2. Processes the appropriate connector.
    3. Publishes the result to the result stream.
    4. Acknowledges successfully processed messages.
    5. Recovers stale pending messages using XAUTOCLAIM.
    """

    def __init__(
        self, block_ms: int = 2000, 
        batch_size: int = 5,
        claim_idle_ms: int = 30000,
        recovery_interval: int = 10,
    ):
        self.stream_name = settings.connector_stream
        self.result_stream = settings.result_stream
        self.group_name = settings.consumer_group
        self.consumer_name = settings.consumer_name

        #separate consumer identity used only for recovery
        self.recovery_consumer_name = (
            f"{self.consumer_name}-recovery"
        )

        self.block_ms = block_ms
        self.batch_size = batch_size

        # a pending message must be idel for this long
        # before another consumer can reclaim it.
        self.claim_idle_ms = claim_idle_ms

        # how frequently we check for stale pending message.
        self.recovery_interval = recovery_interval

        self._is_running = False
        self._stop_event = asyncio.Event()

    async def _setup_consumer_group(self):
        """Creates the Redis Stream and consumer group if they do not already exist."""
        try:
            logger.info(
                f"Creating/checking consumer group "
                f"'{self.group_name}' on stream '{self.stream_name}'..."
            )
            
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

        # -----------------------------------------------------------
        # 1. Pre-flight health check
        # -----------------------------------------------------------
        redis_ok = await check_redis()
        if not redis_ok:
            logger.error("Cannot start worker - Redis is not reachable. Fix connection and restart.")
            return

        # -----------------------------------------------------------
        # 2. Ensure consumer group exists
        # -----------------------------------------------------------
        await self._setup_consumer_group()

        logger.info(
            f"Connector Worker listening on stream '{self.stream_name}' "
            f"[Group: {self.group_name}, Consumer: {self.consumer_name}]"
        )

        logger.info(
            f"Recovery consumer: "
            f"{self.recovery_consumer_name} "
            f"[idle={self.claim_idle_ms}ms, "
            f"interval= {self.recovery_interval}s] "

        )
        
        # -----------------------------------------------------------
        # 3. Ensure consumer group exists
        # -----------------------------------------------------------
        try:
            await asyncio.gather(
                self._consumer_new_messages(),
                self._recover_pending_messages(),
            )

        except asyncio.CancelledError:
            logger.info(
                "Connector Worker received cancellaiton signal."
            )
        
        finally:
            self._is_running = False
        
        logger.info("Connector Worker has exited.")
    

    async def _consumer_new_messages(self):
        """
        Reads NEW messages from the redis stream.
        """
        logger.info("New-message consumer loop started.")

        while self._is_running and not self._stop_event.is_set():
            try:
                entries = await redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={
                        self.stream_name: ">"
                    },
                    count=self.batch_size,
                    block=self.block_ms
                )

                if not entries:
                    continue

                for stream_key, messages in entries:
                    for message_id, raw_fields in messages:
                        await self._process_task(
                            message_id, 
                            raw_fields,
                        )

            except asyncio.CancelledError:
                logger.info(
                    "New-message consumer loop cancelled."
                )
                raise

            except Exception as exc:
                logger.exception(
                    f"Unexpected error in connector worker loop: {exc}"
                )
                await asyncio.sleep(5.0)

        logger.info("New-message consumer loop exited.")

    
    async def _recover_pending_messages(self):
        """
        Recovers stale pending message using Redis XAUTOCLAIM.
        """

        logger.info(
            "Pending-message recovery loop started "
            f"(idle threshold={self.claim_idle_ms}ms)."
        )

        # XAUTOCLAIM pagination cursor.
        # Redis starts scanning the Pending Entries List from here
        claim_start_id = "0-0"

        while self._is_running and not self._stop_event.is_set():
            try:

                result = await redis_client.xautoclaim(
                    name=self.stream_name,
                    groupname=self.group_name,
                    consumername=self.recovery_consumer_name,
                    min_idle_time=self.claim_idle_ms,
                    start_id=claim_start_id,
                    count= self.batch_size,
                )

                next_start_id, messages, deleted_ids = result 

                #continue scanning from Redis' returned cursor.
                claim_start_id = next_start_id

                if messages:
                    logger.info(
                        f"Recovered {len(messages)} stale pending "
                        f"message(s) using XAUTOCLAIM."
                    )

                    for message_id, raw_fields in messages:

                        logger.info(
                            f"Processing recovered message "
                            f"{message_id}."
                        )

                        await self._process_task(
                            message_id,
                            raw_fields,
                        )

                # when redis reaches the end of the PEL scan,
                # restart scanning from the beginning.
                # if claim_start_id == "0-0":
                #     claim_start_id = "0-0"

                # wait before checking again:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.recovery_interval,
                    )

                except asyncio.TimeoutError:
                    # normalL timeout simply means
                    # "Check the PEL again"

                    pass
            
            except asyncio.CancelledError:
                
                logger.info(
                    "Pending-message recovery loop cancelled."
                )
                raise

            except redis.exceptions.ResponseError as exc:

                # if the consumer group does not exist, 
                # don't immediately kill the worker. 

                if "NOGROUP" in str(exc):
                    logger.error(
                        "Redis consumer group doest not exist "
                        f"for XAUTOCLAIM: {exc}"
                    )

                else:
                    logger.exception(
                        f"Redis error during XAUTOCLAIM: {exc}"
                    )

                await asyncio.sleep(5.0)
            
            except Exception as exc:
                logger.exception(
                    f"Unexpected error in pending-message "
                    f"recovery loop: {exc}"
                )
                await asyncio.sleep(5.0)
        
        logger.info("Pending-message recovery loop exited.")


    async def _process_task(
        self, message_id: str, 
        fields: Dict[str, Any],
    ):
        """Processes a single task, scrapes the feed, and acknowledges Redis."""
        request_id = fields.get("request_id")
        source = fields.get("source")
        source_type = fields.get("source_type")
        source_url = fields.get("source_url")
        metadata_raw = fields.get("metadata", {})

        # ---------------------------------------------------------
        # Parse metadata
        # ---------------------------------------------------------
        try:
            metadata = json.loads(metadata_raw)
        
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                f"Invalid metadata for message {message_id}."
                f"Using empty metadata."
            )
            metadata = {}
        
        # ---------------------------------------------------------
        # Validate required fields
        # ---------------------------------------------------------
        if not request_id or not source or not source_type or not source_url:
            
            logger.error(
                f"Message {message_id} is missing."
                f"request_id, source, source_type or source_url: "
                f"{fields}"
            )

            # invalid message is not recoverable.
            # therefore acknowledge it so its doesn't remain
            # permanently pending.
            await redis_client.xack(
                self.stream_name, 
                self.group_name, 
                message_id
            )
            
            return
        
        logger.info(
            f"Processing Task {message_id} -> "
            f"request_id = {request_id}, "
            f"source={source}, "
            f"source_type={source_type}, "
            f"source_url={source_url}"
        )

        # ---------------------------------------------------------
        # Execute connector
        # ---------------------------------------------------------

        try:
            request = ConnectorRequest(
                request_id = request_id,
                source= source,
                source_type= source_type,
                source_url= source_url,
                metadata= metadata,
            )

            # resolve connector based on source_type.
            connector = get_connector(request.source_type)
            
            # Execute source-specific crawling/parsing.
            crawl_result = await connector.fetch(request)

            # Serialize standardized CrawlResult
            payload_json = crawl_result.model_dump_json()

            # -----------------------------------------------------
            # Publish result
            # -----------------------------------------------------
            result_msg_id = await redis_client.xadd(
                self.result_stream, 
                {
                    "payload": payload_json
                },
            )

            logger.info(
                f"Published result for {request_id} to "
                f"'{self.result_stream}' "
                f"(msg_id: {result_msg_id}, "
                f"items: {crawl_result.item_count})"
            )

            # -----------------------------------------------------
            # ACK ONLY AFTER RESULT IS SUCCESSFULLY PUBLISHED
            # -----------------------------------------------------

            await redis_client.xack(
                self.stream_name, 
                self.group_name, 
                message_id
            )

            logger.info(
                f"Successfully processed and acknowledged task "
                f"{message_id} for request {request_id}"
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
    
    worker = ConnectorWorker(
        block_ms=2000, 
        batch_size=5,
        claim_idle_ms=30000,
        recovery_interval=10,
    )

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, worker.stop
            )
        except (NotImplementedError, AttributeError):
            pass

    try:
        await worker.start()
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.stop()
        
        logger.info(
            "Connector worker terminated."
        )


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        print("\Connector Worker terminated by user.")
