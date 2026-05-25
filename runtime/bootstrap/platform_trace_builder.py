from __future__ import annotations

from typing import Mapping

from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec


def _optional_payload_str(payload: Mapping[str, object], key: str) -> str | None:
    raw = payload.get(key)
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    return text or None


def build_platform_trace_spec_from_launch(
    *,
    launch_spec: LaunchSpec,
    launch_payload: Mapping[str, object],
) -> PlatformTraceSpec:
    """Map bootstrap launch context into :class:`PlatformTraceSpec` (composition root)."""
    strategy_id = _optional_payload_str(launch_payload, "strategy_id")
    request_id = _optional_payload_str(launch_payload, "request_id")
    if request_id is None:
        request_id = f"{launch_spec.runtime_id}:{launch_spec.launch_attempt}"
    return PlatformTraceSpec(
        strategy_id=strategy_id,
        strategy_version_id=launch_spec.strategy_version_id,
        correlation_id=launch_spec.correlation_id,
        request_id=request_id,
    )
