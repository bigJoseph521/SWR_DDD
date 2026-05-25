from __future__ import annotations

import pytest
from runtime.domain.enums import RuntimeMode, ServiceTarget
from runtime.domain.errors import (
    WorkerModeNotSupportedError,
    WorkerPolicyViolationError,
)
from runtime.runtime.mode_policy import (
    Capability,
    get_mode_policy,
    require_capability,
)


@pytest.mark.parametrize(
    "mode", [RuntimeMode.PAPER, RuntimeMode.LIVE, RuntimeMode.BACKTEST]
)
def test_all_modes_route_order_intent_to_risk_service(mode: RuntimeMode) -> None:
    policy = get_mode_policy(mode)
    assert policy.route_order_intent() is ServiceTarget.RISK_SERVICE


@pytest.mark.parametrize(
    "mode", [RuntimeMode.PAPER, RuntimeMode.LIVE, RuntimeMode.BACKTEST]
)
def test_replay_chunk_direct_fetch_is_blocked_for_all_modes(mode: RuntimeMode) -> None:
    policy = get_mode_policy(mode)
    assert policy.allow_replay_chunk_direct() is False
    with pytest.raises(WorkerPolicyViolationError) as exc_info:
        policy.require_event_allowed("replay.chunk.direct.fetch")
    assert exc_info.value.details["mode"] == mode.value
    assert exc_info.value.details["event_type"] == "replay.chunk.direct.fetch"


def test_oms_capability_is_allowed_in_backtest() -> None:
    policy = get_mode_policy(RuntimeMode.BACKTEST)
    require_capability(policy, Capability.OMS_EGRESS)


def test_unknown_mode_is_rejected() -> None:
    with pytest.raises(WorkerModeNotSupportedError) as exc_info:
        get_mode_policy("DRY_RUN")  # type: ignore[arg-type]
    assert exc_info.value.details["mode"] == "DRY_RUN"
