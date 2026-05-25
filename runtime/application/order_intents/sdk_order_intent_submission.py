from __future__ import annotations

import logging
from typing import Any, Callable

_LOG = logging.getLogger(__name__)


def safe_submit_sdk_order_intent(
    submit: Callable[[Any], dict[str, Any]],
    intent: Any,
) -> None:
    try:
        submit(intent)
    except Exception:
        _LOG.exception("sdk_order_intent_grpc_submit_failed")
