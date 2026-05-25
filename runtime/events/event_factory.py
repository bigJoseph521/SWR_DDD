from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping

from runtime.events.event_envelope import LifecycleEventEnvelope

_SIGNAL_META: dict[str, tuple[str, str, int, str]] = {
    "bootstrap_succeeded": (
        "runtime.launch_succeeded",
        "worker.lifecycle.bootstrap_succeeded",
        1,
        "bootstrap",
    ),
    "bootstrap_failed": (
        "runtime.launch_failed",
        "worker.lifecycle.bootstrap_failed",
        1,
        "bootstrap",
    ),
    "heartbeat": ("runtime.heartbeat", "worker.lifecycle.heartbeat", 1, "heartbeat"),
    "unhealthy": ("runtime.unhealthy", "worker.lifecycle.unhealthy", 1, "health"),
    "controlled_stop": (
        "runtime.controlled_stop",
        "worker.lifecycle.controlled_stop",
        1,
        "lifecycle",
    ),
    "terminated": ("runtime.terminated", "worker.lifecycle.terminated", 1, "lifecycle"),
}


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _identity_key(*, signal_type: str, payload: Mapping[str, Any]) -> str:
    # Launch failure should be emitted once per runtime launch attempt.
    # Keep a stable identity key so retries/re-emits with different timestamps
    # and details are deduped at ManagerGateway.
    if signal_type == "bootstrap_failed":
        return "bootstrap_failed"
    if "event_id" in payload and isinstance(payload["event_id"], str):
        return payload["event_id"].strip()
    if "observation_id" in payload and isinstance(payload["observation_id"], str):
        return payload["observation_id"].strip()
    canonical_payload = _canonical_json(dict(payload))
    digest = hashlib.sha256(
        f"{signal_type}:{canonical_payload}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{signal_type}:{digest}"


def _event_id(
    *,
    runtime_id: str,
    launch_attempt: int,
    event_name: str,
    identity_key: str,
) -> str:
    digest = hashlib.sha256(
        f"{runtime_id}:{launch_attempt}:{event_name}:{identity_key}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{runtime_id}:{launch_attempt}:{event_name}:{digest}"


def _resolve_occurred_at(payload: Mapping[str, Any]) -> datetime:
    candidate = payload.get("occurred_at")
    if isinstance(candidate, datetime):
        if candidate.tzinfo is None:
            return candidate.replace(tzinfo=timezone.utc)
        return candidate
    observed = payload.get("observed_at")
    if isinstance(observed, datetime):
        if observed.tzinfo is None:
            return observed.replace(tzinfo=timezone.utc)
        return observed
    return datetime.now(timezone.utc)


class LifecycleEventFactory:
    def __init__(self, *, producer: str = "strategy-worker-runtime") -> None:
        self._producer = producer

    def create(
        self,
        *,
        signal_type: str,
        identity: Mapping[str, Any],
        payload: Mapping[str, Any],
        correlation_id: str | None,
        causation_id: str | None,
        triggering_event_id: str | None = None,
    ) -> LifecycleEventEnvelope:
        if signal_type not in _SIGNAL_META:
            raise ValueError(f"Unsupported signal_type: {signal_type}")

        event_name, event_type, event_version, event_family = _SIGNAL_META[signal_type]
        runtime_id = str(identity.get("runtime_id") or "")
        launch_attempt = int(identity.get("launch_attempt") or 0)
        worker_identity = str(identity.get("worker_identity") or "").strip()
        if not worker_identity:
            worker_identity = f"worker:{runtime_id}:{launch_attempt}"

        identity_key = _identity_key(signal_type=signal_type, payload=payload)
        event_id = _event_id(
            runtime_id=runtime_id,
            launch_attempt=launch_attempt,
            event_name=event_name,
            identity_key=identity_key,
        )

        effective_correlation_id = (
            correlation_id or ""
        ).strip() or f"corr:{runtime_id}:{launch_attempt}"
        effective_causation_id = (
            triggering_event_id
            or causation_id
            or f"cmd_{signal_type}_{runtime_id}_{launch_attempt}"
        )

        canonical_payload = dict(payload)
        # Heartbeat Struct payload is only local_state + observed_at (+ optional triggers);
        # Minimal termination (RUNTIME_JOB_COMPLETED) is only local_state + reason_code.
        # identity stays on the protobuf envelope / stdout envelope top level.
        if signal_type != "heartbeat":
            if signal_type == "terminated" and set(payload.keys()) == {
                "local_state",
                "reason_code",
            }:
                pass
            else:
                canonical_payload.setdefault("runtime_id", runtime_id)
                canonical_payload.setdefault("launch_attempt", launch_attempt)
                canonical_payload.setdefault("worker_identity", worker_identity)

        return LifecycleEventEnvelope(
            event_id=event_id,
            event_type=event_type,
            event_name=event_name,
            event_family=event_family,
            event_version=event_version,
            occurred_at=_resolve_occurred_at(payload),
            correlation_id=effective_correlation_id,
            causation_id=effective_causation_id,
            producer=self._producer,
            payload=canonical_payload,
            runtime_id=runtime_id,
            launch_attempt=launch_attempt,
            worker_identity=worker_identity,
            identity_key=identity_key,
        )
