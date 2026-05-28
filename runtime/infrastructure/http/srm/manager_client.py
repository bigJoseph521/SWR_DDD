from __future__ import annotations

from typing import Any, Mapping

from runtime.infrastructure.http.srm.heartbeat import SrmHeartbeatHttpClient
from runtime.infrastructure.http.srm.lifecycle_signal import SrmLifecycleHttpClient

_MANAGER_HTTP_NOT_CONFIGURED = {
    "reason_code": "MANAGER_HTTP_NOT_CONFIGURED",
    "details": (
        "STRATEGY_RUNTIME_MANAGER_BASE_URL must be set for SRM workload signals."
    ),
}


class _NoopManagerClient:
    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True, "signal_type": payload.get("signal_type")}

    def close(self) -> None:
        return None


class SrmHttpManagerClient:
    """All strategy-runtime-manager workload signals use HTTP (heartbeats + lifecycle)."""

    def __init__(
        self,
        *,
        heartbeat_client: SrmHeartbeatHttpClient,
        lifecycle_client: SrmLifecycleHttpClient,
    ) -> None:
        self._heartbeat = heartbeat_client
        self._lifecycle = lifecycle_client

    def emit_signal(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        signal_type = str(envelope.get("signal_type") or "")
        if signal_type in ("heartbeat", "bootstrap_succeeded", "bootstrap_failed"):
            return self._heartbeat.emit_signal(envelope)
        return self._lifecycle.emit_signal(envelope)

    def close(self) -> None:
        self._heartbeat.close()
        self._lifecycle.close()


def build_manager_client(
    *,
    srm_base_url: str = "",
    owner_resource_id: str = "",
    heartbeat_timeout_seconds: float = 30.0,
) -> Any:
    """Build the HTTP client for strategy-runtime-manager workload signals."""
    base_url = srm_base_url.strip()
    if not base_url:
        return _NoopManagerClient()

    heartbeat_client = SrmHeartbeatHttpClient(
        base_url=base_url,
        owner_resource_id=owner_resource_id,
        timeout_seconds=heartbeat_timeout_seconds,
    )
    lifecycle_client = SrmLifecycleHttpClient(
        base_url=base_url,
        timeout_seconds=heartbeat_timeout_seconds,
    )
    return SrmHttpManagerClient(
        heartbeat_client=heartbeat_client,
        lifecycle_client=lifecycle_client,
    )
