from __future__ import annotations

from datetime import datetime, timezone

import pytest
from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import (
    UnsupportedDependencyExpansion,
    WorkerPolicyViolationError,
)
from runtime.integration.clock import (
    SimulatedClock,
    SystemClock,
    build_clock,
)
from runtime.runtime.mode_policy import (
    Capability,
    ModePolicy,
    get_mode_policy,
)


@pytest.mark.parametrize("mode", [RuntimeMode.PAPER, RuntimeMode.LIVE])
def test_paper_live_clock_returns_aware_utc(mode: RuntimeMode) -> None:
    clock = build_clock(get_mode_policy(mode))
    assert isinstance(clock, SystemClock)
    now = clock.now()
    assert now.tzinfo is not None
    assert now.utcoffset() == timezone.utc.utcoffset(now)


def test_backtest_simulated_clock_raises_before_init() -> None:
    clock = build_clock(get_mode_policy(RuntimeMode.BACKTEST))
    assert isinstance(clock, SimulatedClock)
    with pytest.raises(WorkerPolicyViolationError) as exc_info:
        clock.now()
    assert exc_info.value.details["reason"] == "simulated_clock_not_initialized"


def test_backtest_simulated_clock_returns_injected_time() -> None:
    clock = build_clock(get_mode_policy(RuntimeMode.BACKTEST))
    assert isinstance(clock, SimulatedClock)
    simulated_now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
    clock.set_time(simulated_now)
    assert clock.now() == simulated_now


@pytest.mark.parametrize(
    ("mode", "allowed", "expected_capability"),
    [
        (
            RuntimeMode.PAPER,
            frozenset({Capability.SIMULATED_CLOCK}),
            Capability.WALL_CLOCK.value,
        ),
        (
            RuntimeMode.BACKTEST,
            frozenset({Capability.WALL_CLOCK}),
            Capability.SIMULATED_CLOCK.value,
        ),
    ],
)
def test_factory_disallows_illegal_clock_per_mode(
    mode: RuntimeMode,
    allowed: frozenset[Capability],
    expected_capability: str,
) -> None:
    with pytest.raises(UnsupportedDependencyExpansion) as exc_info:
        build_clock(ModePolicy(mode=mode, allowed=allowed))
    assert exc_info.value.details["capability"] == expected_capability
