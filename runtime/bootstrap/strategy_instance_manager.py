from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Callable

from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.application.strategy_execution.strategy_error_boundary import (
    StrategyCallResult,
)
from runtime.domain.enums import WorkerMode
from runtime.domain.errors import SingleAssignmentViolationError


@dataclass(frozen=True, slots=True)
class StrategyAssignmentKey:
    runtime_id: str
    strategy_version_id: str
    tenant_id: str
    mode: WorkerMode
    launch_attempt: int
    validated_parameter_identity: str | None = None
    trader_id: str | None = None
    account_id: str | None = None

    def __post_init__(self) -> None:
        has_trader = self.trader_id is not None
        has_account = self.account_id is not None
        if has_trader == has_account:
            raise ValueError("Exactly one of trader_id or account_id must be set.")

    def deterministic_identity_hash(self) -> str:
        payload = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "runtime_id": self.runtime_id,
            "strategy_version_id": self.strategy_version_id,
            "tenant_id": self.tenant_id,
            "trader_id": self.trader_id,
            "account_id": self.account_id,
            "mode": self.mode.value,
            "launch_attempt": self.launch_attempt,
            "validated_parameter_identity": self.validated_parameter_identity,
        }


@dataclass(slots=True)
class _ActiveStrategyInstance:
    key: StrategyAssignmentKey
    adapter: StrategyAdapter


class StrategyInstanceManager:
    def __init__(self) -> None:
        self._active: _ActiveStrategyInstance | None = None

    def create(
        self,
        assignment_key: StrategyAssignmentKey,
        factory: Callable[[], StrategyAdapter],
    ) -> StrategyAdapter:
        if self._active is not None:
            if self._active.key == assignment_key:
                return self._active.adapter
            raise SingleAssignmentViolationError(
                reason="distinct_active_assignment_conflict"
            )

        try:
            adapter = factory()
        except Exception:
            self._active = None
            raise

        self._active = _ActiveStrategyInstance(key=assignment_key, adapter=adapter)
        return adapter

    def get_active(self) -> StrategyAdapter | None:
        if self._active is None:
            return None
        return self._active.adapter

    def stop(self, adapter: StrategyAdapter) -> StrategyCallResult[object]:
        try:
            return adapter.stop()
        finally:
            self.clear()

    def clear(self) -> None:
        self._active = None
