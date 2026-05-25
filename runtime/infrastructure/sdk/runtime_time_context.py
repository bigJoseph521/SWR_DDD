from __future__ import annotations

from datetime import date

from alphovex_sdk.context.time_context import TimeContext
from alphovex_sdk.typedefs import Timestamp
from runtime.infrastructure.clock.clock import default_today


class RuntimeTimeContext(TimeContext):
    """Delegates to replay ``_SimulatedClockService`` (or any object with ``now`` / ``today``)."""

    __slots__ = ("_clock",)

    def __init__(self, *, clock_service: object) -> None:
        self._clock = clock_service

    def now(self) -> Timestamp:
        now_fn = getattr(self._clock, "now", None)
        if not callable(now_fn):
            raise TypeError("clock service has no callable now()")
        return now_fn()

    def today(self) -> date:
        today_fn = getattr(self._clock, "today", None)
        if callable(today_fn):
            return today_fn()
        return default_today(self.now())
