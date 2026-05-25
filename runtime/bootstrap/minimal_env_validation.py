"""Validate SRM-injected minimal environment before full worker bootstrap.

Reads the process environment only (Kubernetes/SRM injection). Does not load ``.env``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

RUNTIME_ID_ENV_KEYS: Final[tuple[str, ...]] = ("RUNTIME_ID", "SWR_RUNTIME_ID")
SRM_BASE_URL_ENV_KEYS: Final[tuple[str, ...]] = (
    "STRATEGY_RUNTIME_MANAGER_BASE_URL",
    "SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL",
)
DEPLOYMENT_ID_ENV_KEY: Final[str] = "DEPLOYMENT_ID"
MODE_ENV_KEYS: Final[tuple[str, ...]] = ("MODE", "SWR_MODE")

SWR_ENV_VALIDATION_FAILED: Final[str] = "SWR_ENV_VALIDATION_FAILED"
SWR_CONTEXT_FETCH_FAILED: Final[str] = "SWR_CONTEXT_FETCH_FAILED"
SWR_CONTEXT_VALIDATION_FAILED: Final[str] = "SWR_CONTEXT_VALIDATION_FAILED"
SWR_ARTIFACT_NOT_FOUND: Final[str] = "SWR_ARTIFACT_NOT_FOUND"
SWR_ENTRYPOINT_INVALID: Final[str] = "SWR_ENTRYPOINT_INVALID"
SWR_ARTIFACT_DIGEST_MISMATCH: Final[str] = "SWR_ARTIFACT_DIGEST_MISMATCH"
SWR_SDK_COMPATIBILITY_FAILED: Final[str] = "SWR_SDK_COMPATIBILITY_FAILED"
SWR_STOP_REQUESTED: Final[str] = "SWR_STOP_REQUESTED"
SWR_MANUAL_STOP_REQUESTED: Final[str] = "SWR_MANUAL_STOP_REQUESTED"
SWR_RUNTIME_JOB_COMPLETED: Final[str] = "SWR_RUNTIME_JOB_COMPLETED"
SWR_ERROR_DETECTED: Final[str] = "SWR_ERROR_DETECTED"
SWR_UNHEALTHY_EXECUTION_DETECTED: Final[str] = "SWR_UNHEALTHY_EXECUTION_DETECTED"
SWR_CONTROLLED_SHUTDOWN_INITIATED: Final[str] = "SWR_CONTROLLED_SHUTDOWN_INITIATED"
SWR_KUBERNETES_TERMINATION: Final[str] = "SWR_KUBERNETES_TERMINATION"
SWR_SHUTDOWN: Final[str] = "SWR_SHUTDOWN"

RUNTIME_STATUS_STARTING: Final[str] = "STARTING"
RUNTIME_STATUS_RUNNING: Final[str] = "RUNNING"
RUNTIME_STATUS_STOPPING: Final[str] = "STOPPING"
RUNTIME_STATUS_STOPPED: Final[str] = "STOPPED"
RUNTIME_STATUS_FAILED: Final[str] = "FAILED"
RUNTIME_STATUS_STARTUP_FAILED: Final[str] = "STARTUP_FAILED"
HEALTH_STATUS_UNKNOWN: Final[str] = "UNKNOWN"
HEALTH_STATUS_HEALTHY: Final[str] = "HEALTHY"
HEALTH_STATUS_UNHEALTHY: Final[str] = "UNHEALTHY"


@dataclass(frozen=True, slots=True)
class MinimalEnvSnapshot:
    runtime_id: str
    srm_base_url: str
    deployment_id: str
    mode: str


@dataclass(frozen=True, slots=True)
class MinimalEnvValidationResult:
    valid: bool
    snapshot: MinimalEnvSnapshot | None
    field_errors: dict[str, str]
    message: str


def _first_nonempty_env(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""


def validate_minimal_env_from_environ() -> MinimalEnvValidationResult:
    """
    SRM is expected to inject ``runtime_id`` and ``STRATEGY_RUNTIME_MANAGER_BASE_URL``.

    ``DEPLOYMENT_ID`` and ``MODE`` / ``SWR_MODE`` are optional for the status body only.
    """
    field_errors: dict[str, str] = {}
    runtime_id = _first_nonempty_env(*RUNTIME_ID_ENV_KEYS)
    if not runtime_id:
        field_errors["runtime_id"] = (
            "required_field_missing: set RUNTIME_ID or SWR_RUNTIME_ID"
        )

    srm_base_url = _first_nonempty_env(*SRM_BASE_URL_ENV_KEYS)
    if not srm_base_url:
        field_errors["STRATEGY_RUNTIME_MANAGER_BASE_URL"] = (
            "required_field_missing: set STRATEGY_RUNTIME_MANAGER_BASE_URL "
            "or SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL"
        )

    if field_errors:
        missing = ", ".join(sorted(field_errors))
        return MinimalEnvValidationResult(
            valid=False,
            snapshot=(
                MinimalEnvSnapshot(
                    runtime_id=runtime_id,
                    srm_base_url=srm_base_url,
                    deployment_id=_first_nonempty_env(DEPLOYMENT_ID_ENV_KEY),
                    mode=_first_nonempty_env(*MODE_ENV_KEYS) or "LIVE",
                )
                if runtime_id and srm_base_url
                else None
            ),
            field_errors=field_errors,
            message=f"Minimal environment validation failed: {missing}",
        )

    deployment_id = _first_nonempty_env(DEPLOYMENT_ID_ENV_KEY)
    mode = _first_nonempty_env(*MODE_ENV_KEYS) or "LIVE"
    return MinimalEnvValidationResult(
        valid=True,
        snapshot=MinimalEnvSnapshot(
            runtime_id=runtime_id,
            srm_base_url=srm_base_url,
            deployment_id=deployment_id,
            mode=mode,
        ),
        field_errors={},
        message="Minimal environment validation passed.",
    )
