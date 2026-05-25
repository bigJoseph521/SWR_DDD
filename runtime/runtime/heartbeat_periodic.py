from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Final

_logger = logging.getLogger(__name__)

# Wall-clock interval for HTTP runtime status to strategy-runtime-manager (POST .../status).
HEARTBEAT_INTERVAL_SECONDS: Final[float] = 10.0


class HeartbeatPeriodicJobs:
    """Background loop: emit manager heartbeats at a fixed wall-clock interval."""

    def __init__(
        self,
        *,
        on_tick: Callable[[], None],
        interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive.")
        self._on_tick = on_tick
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="swr-heartbeat", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        # First heartbeat is emitted synchronously at launch (LifecycleService.start).
        while not self._stop.is_set():
            if self._stop.wait(self._interval):
                break
            try:
                self._on_tick()
            except Exception:
                _logger.exception("heartbeat tick failed")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 2.0)
            self._thread = None
