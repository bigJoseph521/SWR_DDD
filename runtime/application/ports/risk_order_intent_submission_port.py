from __future__ import annotations

from typing import Any, Mapping, Protocol

from runtime.domain.model.strategy_order_intent import StrategyOrderIntent


class RiskOrderIntentSubmissionPort(Protocol):
    """Submit strategy order intents to Risk Service (``risk_worker.proto``)."""

    def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]: ...
