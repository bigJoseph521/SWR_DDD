from __future__ import annotations

import pytest
from runtime.bootstrap.strategy_adapter import StrategyAdapter
from runtime.bootstrap.strategy_instance_manager import (
    StrategyAssignmentKey,
    StrategyInstanceManager,
)
from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import SingleAssignmentViolationError


def _key(*, runtime_id: str = "rt-1", launch_attempt: int = 1) -> StrategyAssignmentKey:
    return StrategyAssignmentKey(
        runtime_id=runtime_id,
        strategy_version_id="sv-1",
        tenant_id="tenant-1",
        mode=RuntimeMode.PAPER,
        launch_attempt=launch_attempt,
        account_id="acct-1",
        validated_parameter_identity="vp-1",
    )


def test_first_create_succeeds() -> None:
    manager = StrategyInstanceManager()
    adapter = StrategyAdapter(strategy=object())

    created = manager.create(_key(), lambda: adapter)

    assert created is adapter
    assert manager.get_active() is adapter


def test_second_create_while_active_raises_conflict() -> None:
    manager = StrategyInstanceManager()
    manager.create(_key(runtime_id="rt-1"), lambda: StrategyAdapter(strategy=object()))

    with pytest.raises(SingleAssignmentViolationError):
        manager.create(
            _key(runtime_id="rt-2"), lambda: StrategyAdapter(strategy=object())
        )


def test_stop_clears_active_instance() -> None:
    manager = StrategyInstanceManager()
    adapter = StrategyAdapter(strategy=object())
    manager.create(_key(), lambda: adapter)

    result = manager.stop(adapter)

    assert result.ok is True
    assert manager.get_active() is None


def test_init_failure_does_not_retain_active_state() -> None:
    manager = StrategyInstanceManager()

    def _failing_factory() -> StrategyAdapter:
        raise RuntimeError("factory_failed")

    with pytest.raises(RuntimeError):
        manager.create(_key(), _failing_factory)

    assert manager.get_active() is None


def test_cleanup_called_exactly_once() -> None:
    manager = StrategyInstanceManager()

    class _Strategy:
        def __init__(self) -> None:
            self.calls = 0

        def stop(self) -> None:
            self.calls += 1

    strategy = _Strategy()
    adapter = StrategyAdapter(strategy=strategy)
    manager.create(_key(), lambda: adapter)

    manager.stop(adapter)

    assert strategy.calls == 1
    assert manager.get_active() is None


def test_cleanup_exception_still_clears_active_state() -> None:
    manager = StrategyInstanceManager()

    class _Strategy:
        def stop(self) -> None:
            raise RuntimeError("cleanup_failed")

    adapter = StrategyAdapter(strategy=_Strategy())
    manager.create(_key(), lambda: adapter)

    result = manager.stop(adapter)

    assert result.ok is False
    assert result.reason_code == "STRATEGY_CLEANUP_UNHANDLED_EXCEPTION"
    assert manager.get_active() is None
