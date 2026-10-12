import logging 
import asyncio 

from app.core.config import settings
from app.services.scheduler_service import dispatch_due_units_once

logger = logging.getLogger(__name__)

class SchedulerWorker:
    """Runs the scheduler dispatcher periodically."""

    def __init__(self, tick_seconds: int | None = None):
        self.tick_seconds = (
            settings.dispatch_tick_seconds
            if tick_seconds is None
            else tick_seconds
        )

        if self.tick_seconds <= 0:
            raise ValueError("Tick_seconds must be greater than zero.")

        self._stop_event = asyncio.Event()

    
    async def run_once(self) -> int:
        """Execute one dispatcher tick."""

        dispatched_count = await dispatch_due_units_once()

        logger.info(
            "Scheduler tick completed. Dispatched %s units.",
            dispatched_count,
        )

        return dispatched_count


    async def run(self) -> None:
        """Run dispatcher tick until shutdown is requested."""


        self._stop_event.clear()

        logger.info(
            "Scheduler worker started. Tick interval: %s seconds.",
            self.tick_seconds,
        )

        while not self._stop_event.is_set():
            try:
                await self.run_once()
            
            except asyncio.CancelledError:
                logger.info("Scheduler worker received cancellation signal.")
                raise

            except Exception:
                logger.exception("Scheduler tick failed.")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.tick_seconds,
                )

            except asyncio.TimeoutError:
                pass
        
        logger.info("Scheduler worker stopped.")

    async def stop(self) -> None:
        """
        Request a graceful shutdown.
        """

        self._stop_event.set()


        