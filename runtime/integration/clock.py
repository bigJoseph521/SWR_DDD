from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Protocol

from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import (
    WorkerModeNotSupportedError,
    WorkerPolicyViolationError,
)
from runtime.runtime.mode_policy import (
    Capability,
    ModePolicy,
    require_capability,
)


def default_today(now_ts: datetime) -> date:
    """Calendar date in the timezone of ``now_ts`` (SDK clock helper equivalent)."""
    return now_ts.date()


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class SimulatedClock:
    def __init__(self) -> None:
        self._current_time: datetime | None = None

    def set_time(self, value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise WorkerPolicyViolationError(
                policy="simulated_clock_requires_aware_utc",
                reason="simulated_time_must_be_timezone_aware",
            )
        self._current_time = value.astimezone(timezone.utc)

    def now(self) -> datetime:
        if self._current_time is None:
            raise WorkerPolicyViolationError(
                policy="simulated_clock_initialization",
                reason="simulated_clock_not_initialized",
            )
        return self._current_time


def build_clock(policy: ModePolicy) -> Clock:
    if policy.mode in (RuntimeMode.PAPER, RuntimeMode.LIVE):
        require_capability(policy, Capability.WALL_CLOCK)
        return SystemClock()

    if policy.mode is RuntimeMode.BACKTEST:
        require_capability(policy, Capability.SIMULATED_CLOCK)
        return SimulatedClock()

    raise WorkerModeNotSupportedError(mode=policy.mode.value)
