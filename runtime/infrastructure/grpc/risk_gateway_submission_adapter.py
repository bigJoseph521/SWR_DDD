from __future__ import annotations

import logging
from typing import Any, Mapping

from runtime.domain.errors import OrderIntentWireMappingError
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
from runtime.infrastructure.grpc.dependency_client_error import DependencyClientError
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.infrastructure.grpc.risk_order_intent_submission_errors import (
    dependency_client_error_to_submission_error,
    misconfigured_client_error,
)
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
    build_risk_order_intent_wire_payload,
)

_LOG = logging.getLogger(__name__)


def _coerce_result_dict(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return dict(result)
    return {"result": result}


class RiskGatewaySubmissionAdapter:
    """Infrastructure adapter: Risk gRPC gateway → :class:`RiskOrderIntentSubmissionPort`."""

    def __init__(
        self,
        gateway: RiskOrderIntentGateway,
        *,
        submission_context: OrderSubmissionContext,
    ) -> None:
        self._gateway = gateway
        self._submission_context = submission_context
        self.last_wire_payload: dict[str, Any] | None = None

    def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
        wire: dict[str, Any] | None = None
        try:
            wire = build_risk_order_intent_wire_payload(
                intent,
                context=self._submission_context,
                created_at=intent.created_at,
            )
        except OrderIntentWireMappingError as exc:
            _LOG.error(
                "risk_order_intent_wire_mapping_failed",
                extra={
                    "reason_code": exc.reason_code,
                    **exc.diagnostics,
                },
            )
            raise
        self.last_wire_payload = wire
        try:
            return _coerce_result_dict(self._gateway.submit_order_intent_wire(wire))
        except DependencyClientError as exc:
            raise dependency_client_error_to_submission_error(exc, wire=wire) from exc
        except TypeError as exc:
            _LOG.error(
                "risk_order_intent_client_misconfigured",
                extra={"instrument_id": intent.instrument_id},
                exc_info=False,
            )
            raise misconfigured_client_error(wire=wire) from exc


# Backward-compatible name used by tests and wiring code.
RiskGatewaySubmissionPort = RiskGatewaySubmissionAdapter
