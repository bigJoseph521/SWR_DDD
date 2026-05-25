from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping

from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.domain.policies.mode_policy import (
    Capability,
    ModePolicy,
    require_capability,
)


class ReplayGateway:
    def __init__(
        self,
        policy: ModePolicy,
        *,
        simulated_clock: SimulatedClock | None = None,
        on_first_data: Callable[[], None] | None = None,
        tick_logging_quiet: bool = False,
    ) -> None:
        require_capability(policy, Capability.REPLAY_INGRESS)
        self._policy = policy
        self._simulated_clock = simulated_clock
        self._on_first_data = on_first_data
        self._tick_logging_quiet = tick_logging_quiet
        self._first_data_marked = False
        self._tick_count = 0

    def ingest_replay_tick(
        self,
        tick: Mapping[str, Any],
        *,
        strategy_callback: Callable[[Mapping[str, Any]], Any] | None = None,
        simulated_time: datetime | None = None,
    ) -> Any:
        """
        Advance simulated clock and optional first-data callback.

        ``strategy_callback`` is deprecated: market events must go through
        :class:`runtime.application.event_handling.event_dispatcher.EventDispatcher`.
        """
        if self._simulated_clock is not None and simulated_time is not None:
            self._simulated_clock.set_time(simulated_time)

        normalized_tick = dict(tick)
        self._tick_count += 1
        if not self._tick_logging_quiet:
            if self._tick_count == 1:
                eid = normalized_tick.get("event_id", "")
                sym = normalized_tick.get("symbol") or normalized_tick.get(
                    "instrument_id", ""
                )
                print(
                    f"[replay] data arrived (first tick): event_id={eid!r} symbol={sym!r}",
                    flush=True,
                )
            elif self._tick_count % 100 == 0:
                print(
                    f"[replay] data arrived: tick_count={self._tick_count}", flush=True
                )
        if not self._first_data_marked and self._on_first_data is not None:
            self._on_first_data()
            self._first_data_marked = True

        if strategy_callback is None:
            return normalized_tick
        return strategy_callback(normalized_tick)
