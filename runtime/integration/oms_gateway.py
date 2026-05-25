from __future__ import annotations

import json
import logging
from typing import Any, Callable, Mapping

from runtime.domain.order_intent import OrderIntent
from runtime.runtime.mode_policy import (
    Capability,
    ModePolicy,
    require_capability,
)
from runtime.transport.grpc.oms_client import (
    cancel_order_intent_wire_dict_for_console,
    order_intent_wire_dict_for_console,
    replace_order_intent_wire_dict_for_console,
)

_LOG = logging.getLogger(__name__)

_ORDER_INTENT_CLI_BANNER = "---------order intent from worker runtime--------------"
_CANCEL_INTENT_CLI_BANNER = (
    "---------cancel order intent from worker runtime--------------"
)
_REPLACE_INTENT_CLI_BANNER = (
    "---------replace order intent from worker runtime--------------"
)


def _print_order_intent_to_console(payload: Mapping[str, Any]) -> None:
    wire = order_intent_wire_dict_for_console(payload)
    print(_ORDER_INTENT_CLI_BANNER, flush=True)
    print(
        json.dumps(wire, indent=2, ensure_ascii=True, sort_keys=True),
        flush=True,
    )


def _print_cancel_order_intent_to_console(payload: Mapping[str, Any]) -> None:
    wire = cancel_order_intent_wire_dict_for_console(payload)
    print(_CANCEL_INTENT_CLI_BANNER, flush=True)
    print(
        json.dumps(wire, indent=2, ensure_ascii=True, sort_keys=True),
        flush=True,
    )


def _print_replace_order_intent_to_console(payload: Mapping[str, Any]) -> None:
    wire = replace_order_intent_wire_dict_for_console(payload)
    print(_REPLACE_INTENT_CLI_BANNER, flush=True)
    print(
        json.dumps(wire, indent=2, ensure_ascii=True, sort_keys=True),
        flush=True,
    )


def _coerce_submit_result_dict(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return dict(result)
    return {"result": result}


class OmsGateway:
    def __init__(
        self,
        policy: ModePolicy,
        oms_client: object,
        *,
        on_order_intent_result: (
            Callable[[str, dict[str, Any], dict[str, Any]], None] | None
        ) = None,
    ) -> None:
        require_capability(policy, Capability.OMS_EGRESS)
        self._policy = policy
        self._oms_client = oms_client
        self._on_order_intent_result = on_order_intent_result

    def _notify_order_intent_result(
        self, payload: Mapping[str, Any], result: Any
    ) -> None:
        cb = self._on_order_intent_result
        if cb is None:
            return
        try:
            cb("oms", dict(payload), _coerce_submit_result_dict(result))
        except Exception:
            _LOG.exception("oms_order_intent_journal_callback_failed")

    def submit_order_intent(self, intent: OrderIntent) -> Any:
        intent.validate_for_oms_submission()
        normalized_payload = intent.to_dict()
        _print_order_intent_to_console(normalized_payload)
        sender = getattr(self._oms_client, "submit_order_intent", None)
        if not callable(sender):
            raise TypeError("OMS client must expose submit_order_intent(payload).")
        out = sender(normalized_payload)
        self._notify_order_intent_result(normalized_payload, out)
        return out

    def submit_order_intent_payload(self, payload: Mapping[str, Any]) -> Any:
        """
        Submit a pre-serialized intent mapping (e.g. from SDK OrderIntent) to the OMS
        gRPC client without constructing the domain :class:`OrderIntent` model.
        """
        normalized_payload = dict(payload)
        _print_order_intent_to_console(normalized_payload)
        sender = getattr(self._oms_client, "submit_order_intent", None)
        if not callable(sender):
            raise TypeError("OMS client must expose submit_order_intent(payload).")
        out = sender(normalized_payload)
        self._notify_order_intent_result(normalized_payload, out)
        return out

    def submit_replace_order_intent_payload(self, payload: Mapping[str, Any]) -> Any:
        """
        Submit a replace-intent mapping via gRPC (``ReplaceOrderIntent`` /
        ``SubmitReplaceOrderIntent`` on risk-service).
        """
        normalized_payload = dict(payload)
        _print_replace_order_intent_to_console(normalized_payload)
        sender = getattr(self._oms_client, "submit_replace_order_intent", None)
        if not callable(sender):
            raise TypeError(
                "OMS client must expose submit_replace_order_intent(payload)."
            )
        out = sender(normalized_payload)
        self._notify_order_intent_result(normalized_payload, out)
        return out

    def submit_cancel_order_intent_payload(self, payload: Mapping[str, Any]) -> Any:
        """
        Submit a cancel-intent mapping via gRPC (``CancelOrderIntent`` /
        ``SubmitCancelOrderIntent`` on risk-service).
        """
        normalized_payload = dict(payload)
        _print_cancel_order_intent_to_console(normalized_payload)
        sender = getattr(self._oms_client, "submit_cancel_order_intent", None)
        if not callable(sender):
            raise TypeError(
                "OMS client must expose submit_cancel_order_intent(payload)."
            )
        out = sender(normalized_payload)
        self._notify_order_intent_result(normalized_payload, out)
        return out
