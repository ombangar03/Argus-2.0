# Outbound dispatcher
"""
What is It About? (Why Did We Need It?)
Before creating 

request_worker.py ,we already had 

request_service.py
.

However, request_service.py was just a passive set of instructions — like an engine sitting on the floor. Every time you wanted to claim and send a request, a human had to open a terminal and manually run python -m app.services.request_service.

request_worker.py
 gives your Orchestrator autonomous, non-stop life. It is the process that sits in the background 24/7, watching for work, doing the work, and resting when there is no work.

What Does It Actually Do? (Summary of Responsibilities)
Gatekeeper (Pre-flight checks)
Before doing anything, it tests MongoDB and Redis. If either is down, it halts immediately with a clear error rather than crashing in a loop.
The Polling Loop (Autonomous Execution)
It continuously asks MongoDB: "Do we have any new RSS feeds marked pending?"
Smart Elastic Pacing (Drain Mode vs. Idle Mode):
If there is work (e.g., 50 pending feeds): It runs at full speed without sleeping, dispatching all 50 tasks to Redis in a fraction of a second.
If there is no work: It sleeps for 2 seconds (configurable) so your computer doesn't waste CPU or flood MongoDB with useless queries.
Resilience & Self-Healing
If Redis or the network blips temporarily, it catches the error, logs it, pauses for 5 seconds, and retries automatically. It never crashes silently.
Clean Shutdown
When you press Ctrl+C (or stop a Docker container), it safely finishes the current operation and stops cleanly without corrupting database records.
"""


import asyncio 
import logging
import signal 
from typing import Optional

from app.core.logging import setup_logging
from app.platform.mongodb import check_mongodb
from app.platform.redis import check_redis
from app.services.request_service import dispatch_rss_request

logger = logging.getLogger(__name__)

class RequestWorker:
    """
    Continous background worker that polls MongoDB for per pending RSS requests
    and dispatches them to Redis Stream using dispatch_rss_request().
    """

    def __init__(self, poll_interval: float = 2.0):
        """
        :param poll_interval: seconds to wait before checking MongoDB again when queue is empty.
        """
        self.poll_interval = poll_interval
        self._is_running = False
        self._stop_event = asyncio.Event()


    async def start(self):
        """
        Starts the continuous worker loop.
        Verfies database and broker health, then enters the polling loop.
        """

        self._is_running = True
        self._stop_event.clear()

        logger.info("Initializing Request Worker....")

        # 1. Verify connections to MongoDB and Redis before doing any work
        mongo_ok = await check_mongodb()
        redis_ok = await check_redis()

        if not mongo_ok or not redis_ok:
            logger.error("Database or Redis is not Healthy.")
            return 

        logger.info(f"Request worker started successfully. Polling interval: {self.poll_interval}s")

        # 2. Main processing Loop
        while self._is_running and not self._stop_event.is_set():
            try:
                # Atomically claim and dispatch one request from MongoDB to Redis
                dispatched = await dispatch_rss_request()

                if dispatched:
                    logger.info(f"Successfully dispatched request: {dispatched.request_id}")
                    # if work was found, loop immediately to drain the queue quickly
                    await asyncio.sleep(0.01)

                else:
                    # No pending work found. Sleep or poll_interval (or wake immdiately on stop_event)
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=self.poll_interval
                        )
                    except (asyncio.TimeoutError, TimeoutError):
                        # Normal behavior: The poll interval elapsed, check again
                        pass

            except asyncio.CancelledError:
                logger.info("Request Worker received cancellation signal.")
                break 

            except Exception as exc:
                logger.exception(f"Unexpexted error in Request Worker loop: {exc}")
                # wait briefly before retrying to prevent rapid crash loop
                await asyncio.sleep(5.0)

        logger.info("Request Worker loop has exited.")


    def stop(self):
        """
        Gracefully signals the worker loop to stop running.
        """

        logger.info("Stopping Request Worker gracefully...")
        self._is_running = False
        self._stop_event.set()


async def run_worker():
    """
    Entrypoint to set up logging, create the worker, and handle graceful shutdown.
    """
    setup_logging()
    worker = RequestWorker(poll_interval=2.0)

    # register OS signal handlers for gracefull shutdown.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        
        except (NotImplementedError, AttributeError):
            # windows ProactorEventLoop does not support add_signal_handler
            pass 

    try:
        await worker.start()

    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.stop()
        logger.info("Request Worker has stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    
    except KeyboardInterrupt:
        print("\nRequest Worker terminated by user.")