from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from runtime.application.order_intents.submit_order_intent import SubmitOrderIntent
from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.enums import WorkerMode
from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.errors import (
    OrderIntentWireMappingError,
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.backtest.backtest_stdout_order_intent_submission_adapter import (
    BacktestStdoutOrderIntentSubmissionAdapter,
)
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.infrastructure.grpc.risk_gateway_submission_adapter import (
    RiskGatewaySubmissionAdapter,
)
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
    build_risk_order_intent_wire_payload,
    strategy_order_intent_from_sdk,
)

_LOG = logging.getLogger(__name__)


def _normalize_utc_dt(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _intent_created_at_sim(sdk_intent: Any, clock: Any) -> datetime:
    created = getattr(sdk_intent, "created_at", None)
    if isinstance(created, datetime):
        return _normalize_utc_dt(created)
    now_fn = getattr(clock, "now", None)
    if callable(now_fn):
        try:
            t = now_fn()
            if isinstance(t, datetime):
                return _normalize_utc_dt(t)
        except Exception:
            _LOG.debug(
                "intent_created_at_clock_unavailable_using_wall_time",
                exc_info=False,
            )
    return datetime.now(timezone.utc)


def _resolve_created_at_for_sdk_intent(
    sdk_intent: Any,
    clock: Any,
    *,
    latest_market_event_at: Callable[[], datetime | None] | None,
) -> datetime:
    if latest_market_event_at is not None:
        try:
            snap = latest_market_event_at()
        except Exception:
            _LOG.debug("intent_created_at_market_snapshot_failed", exc_info=False)
        else:
            if isinstance(snap, datetime):
                return _normalize_utc_dt(snap)
    return _intent_created_at_sim(sdk_intent, clock)


def build_sdk_order_intent_submitter(
    *,
    dependencies: RuntimeDependencies,
    launch_spec: LaunchSpec,
    worker_identity: WorkerIdentity,
    on_order_intent_result: (
        Callable[[Literal["risk", "backtest"], dict[str, Any], dict[str, Any]], None]
        | None
    ) = None,
    order_intent_correlation_id: str = "",
    disable_order_intent_grpc: bool = False,
    latest_market_event_at: Callable[[], datetime | None] | None = None,
    allocate_order_intent_id: Callable[[], str] | None = None,
    platform_trace: PlatformTraceSpec | None = None,
) -> Callable[[Any], dict[str, Any]] | None:
    """
    Composition-root helper: SDK order intent → application use case → mode egress.

    BACKTEST writes ``ORDER_INTENT`` JSONL lines to stdout. PAPER/LIVE submit to Risk Service.
    """
    _ = worker_identity
    env_corr = order_intent_correlation_id
    if platform_trace is not None:
        corr_fallback = (
            platform_trace.effective_correlation_id(env_fallback=env_corr) or ""
        )
    else:
        corr_fallback = env_corr

    if allocate_order_intent_id is not None:
        next_order_intent_id: Callable[[], str] = allocate_order_intent_id
    else:

        def next_order_intent_id() -> str:
            return str(uuid.uuid4())

    submission_context = OrderSubmissionContext(
        launch_spec=launch_spec,
        correlation_id_fallback=corr_fallback,
        platform_trace=platform_trace,
        allocate_order_intent_id=next_order_intent_id,
    )

    is_backtest = launch_spec.mode is WorkerMode.BACKTEST
    backtest_port = BacktestStdoutOrderIntentSubmissionAdapter(
        submission_context=submission_context,
    )
    backtest_use_case = SubmitOrderIntent(submission_port=backtest_port)

    gateway = dependencies.risk_order_intent
    risk_submission_port: RiskGatewaySubmissionAdapter | None = None
    risk_use_case: SubmitOrderIntent | None = None
    can_submit_risk = False
    if isinstance(gateway, RiskOrderIntentGateway):
        risk_inner = getattr(gateway, "_risk_client", None)
        can_submit_risk = callable(getattr(risk_inner, "submit_order_intent", None))
        risk_submission_port = RiskGatewaySubmissionAdapter(
            gateway,
            submission_context=submission_context,
        )
        risk_use_case = SubmitOrderIntent(submission_port=risk_submission_port)

    if is_backtest:
        submit_use_case = backtest_use_case
        submission_port = backtest_port
        egress_source: Literal["risk", "backtest"] = "backtest"
    elif risk_use_case is not None and risk_submission_port is not None:
        submit_use_case = risk_use_case
        submission_port = risk_submission_port
        egress_source = "risk"
    else:
        return None

    def _submit(sdk_intent: Any) -> dict[str, Any]:
        _unavailable = {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
        }
        wire_payload: dict[str, Any] = {}
        result: dict[str, Any] = dict(_unavailable)
        try:
            created_at = _resolve_created_at_for_sdk_intent(
                sdk_intent,
                dependencies.clock,
                latest_market_event_at=latest_market_event_at,
            )
            strategy_intent = strategy_order_intent_from_sdk(
                sdk_intent
            ).with_created_at(created_at)
            if is_backtest:
                outcome = submit_use_case.execute(strategy_intent)
                wire_payload = dict(submission_port.last_wire_payload or {})
                if outcome.response is not None:
                    result = dict(outcome.response)
                elif not outcome.ok:
                    result = {
                        "accepted": False,
                        "error_code": outcome.error_code,
                        "reason_code": outcome.reason_code,
                    }
                else:
                    result = dict(_unavailable)
            elif disable_order_intent_grpc or not can_submit_risk:
                wire_payload = build_risk_order_intent_wire_payload(
                    strategy_intent,
                    context=submission_context,
                    created_at=created_at,
                )
                result = dict(_unavailable)
            else:
                outcome = submit_use_case.execute(strategy_intent)
                wire_payload = dict(submission_port.last_wire_payload or {})
                if outcome.response is not None:
                    result = dict(outcome.response)
                elif not outcome.ok:
                    result = {
                        "accepted": False,
                        "error_code": outcome.error_code,
                        "reason_code": outcome.reason_code,
                    }
                else:
                    result = dict(_unavailable)
        except OrderIntentWireMappingError as exc:
            _LOG.error(
                "order_intent_submit_prepare_failed",
                extra={"reason_code": exc.reason_code, **exc.diagnostics},
            )
            wire_payload = {
                "_prepare_failed": True,
                "runtime_id": launch_spec.runtime_id,
                "instrument_id": str(getattr(sdk_intent, "instrument_id", "") or ""),
                "symbol": (launch_spec.symbol or "").strip(),
                "reason_code": exc.reason_code,
            }
            result = {
                "accepted": False,
                "error_code": "order_intent_wire_mapping_failed",
                "reason_code": exc.reason_code,
            }
        except Exception:
            _LOG.error("order_intent_submit_prepare_failed", exc_info=False)
            wire_payload = {
                "_prepare_failed": True,
                "runtime_id": launch_spec.runtime_id,
                "instrument_id": str(getattr(sdk_intent, "instrument_id", "") or ""),
                "symbol": (launch_spec.symbol or "").strip(),
            }
            result = dict(_unavailable)
        if on_order_intent_result is not None:
            try:
                on_order_intent_result(egress_source, wire_payload, result)
            except Exception:
                _LOG.error("on_order_intent_result_callback_failed", exc_info=False)
        return result

    if is_backtest:
        return _submit
    if (
        not disable_order_intent_grpc and can_submit_risk
    ) or on_order_intent_result is not None:
        return _submit
    return None
