"""In-memory per-event order intent collector for stdio backtest subprocess."""

from __future__ import annotations

from dataclasses import dataclass, field

from runtime.infrastructure.strategy_loader.runtime_stub_support import (
    RuntimeOrderIntent,
)


@dataclass(slots=True)
class StdioOrderIntentCollector:
    """
    Captures SDK order submissions during one ``MARKET_DATA_EVENT`` handling cycle.

    Wired as ``submit_sdk_order_intent`` on :class:`RuntimeSdkBridge`.
    """

    _intents: list[RuntimeOrderIntent] = field(default_factory=list)
    _client_id_seq: int = 0

    def submit(self, intent: RuntimeOrderIntent) -> dict[str, object]:
        self._intents.append(intent)
        return {"accepted": True, "queued": len(self._intents)}

    def drain(self) -> list[RuntimeOrderIntent]:
        captured = list(self._intents)
        self._intents.clear()
        return captured

    def allocate_client_order_id(self, intent: RuntimeOrderIntent) -> str:
        existing = intent.client_order_id
        if existing is not None and str(existing).strip():
            return str(existing).strip()
        self._client_id_seq += 1
        return f"co-{self._client_id_seq:06d}"
