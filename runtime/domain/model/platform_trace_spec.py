from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class PlatformTraceSpec:
    """Observability and audit metadata (not used by strategy calculation)."""

    strategy_id: str | None
    strategy_version_id: str | None
    correlation_id: str | None
    request_id: str | None

    def effective_correlation_id(self, *, env_fallback: str = "") -> str | None:
        """Bundle ``correlation_id`` first, then env/tuning fallback (e.g. order-intent override)."""
        bundle = (self.correlation_id or "").strip()
        if bundle:
            return bundle
        env = (env_fallback or "").strip()
        return env or None

    def as_log_fields(self) -> dict[str, str]:
        """Structured log / domain-event fields (omit unset values)."""
        fields: dict[str, str] = {}
        if self.strategy_id:
            fields["strategy_id"] = self.strategy_id
        if self.strategy_version_id:
            fields["strategy_version_id"] = self.strategy_version_id
        corr = (self.correlation_id or "").strip()
        if corr:
            fields["correlation_id"] = corr
        if self.request_id:
            fields["request_id"] = self.request_id
        return fields

    def merge_event_extras(
        self,
        extras: Mapping[str, Any] | None,
        *,
        env_correlation_fallback: str = "",
    ) -> dict[str, Any]:
        """Merge trace identity onto domain-event extras without overwriting explicit keys."""
        merged = dict(extras or {})
        for key, value in self.as_log_fields().items():
            merged.setdefault(key, value)
        corr = self.effective_correlation_id(env_fallback=env_correlation_fallback)
        if corr:
            merged.setdefault("correlation_id", corr)
        return merged
