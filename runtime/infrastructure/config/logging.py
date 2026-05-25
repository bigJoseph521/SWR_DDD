from __future__ import annotations

import json
import logging
import sys
from typing import Mapping

from runtime.bootstrap.platform_trace_builder import build_platform_trace_spec_from_launch
from runtime.infrastructure.config.settings import Settings
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.observability.logger import RuntimeLogContext


class StructuredEventFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", None)
        if isinstance(event, dict):
            payload = {"level": record.levelname, **event}
        else:
            payload = {"level": record.levelname, "message": record.getMessage()}
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("strategy_worker_runtime")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredEventFormatter())
        logger.addHandler(handler)
    return logger


def build_runtime_log_context(
    *,
    settings: Settings,
    worker_identity: WorkerIdentity,
) -> RuntimeLogContext:
    launch_spec = settings.launch_spec
    trace = build_platform_trace_spec_from_launch(
        launch_spec=launch_spec,
        launch_payload=settings.launch_payload,
    )
    payload = settings.launch_payload
    deployment_id = _optional_payload_str(payload, "deployment_id")
    portfolio_id = _optional_payload_str(payload, "portfolio_id")
    risk_snapshot_id = _optional_payload_str(payload, "risk_snapshot_id")
    return RuntimeLogContext(
        runtime_id=launch_spec.runtime_id,
        tenant_id=launch_spec.tenant_id,
        worker_identity=(
            f"{worker_identity.runtime_id}:{worker_identity.strategy_version_id}:"
            f"{worker_identity.launch_attempt}"
        ),
        launch_attempt=launch_spec.launch_attempt,
        correlation_id=trace.correlation_id,
        account_id=launch_spec.account_id,
        strategy_version_id=trace.strategy_version_id,
        strategy_id=trace.strategy_id,
        deployment_id=deployment_id,
        portfolio_id=portfolio_id,
        risk_snapshot_id=risk_snapshot_id,
        request_id=trace.request_id,
        runtime_type="STRATEGY_WORKER_RUNTIME",
        mode=launch_spec.mode.value,
        job_id=launch_spec.job_id,
    )


def _optional_payload_str(payload: Mapping[str, object], key: str) -> str | None:
    raw = payload.get(key)
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    return text or None
