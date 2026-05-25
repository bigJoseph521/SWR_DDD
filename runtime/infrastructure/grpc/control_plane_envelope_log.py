"""
Worker→runtime-manager observability and JSON shape (where this package builds JSON):

Stdout / logging use one event envelope: top-level ``event_id``, ``event_name``,
``event_version``, ``producer``, ``occurred_at``, ``correlation_id``,
``tenant_id``, ``account_id``, ``runtime_id``, ``worker_identity``, ``launch_attempt``,
``strategy_version_id``, plus a single nested ``payload`` for domain fields.

Paths:
- ``integration.manager_gateway`` + ``ManagerGateway._print_runtime_manager_message``:
  prints the same envelope shape on stdout. When ``STRATEGY_RUNTIME_MANAGER_BASE_URL`` is set,
  ``transport.manager_client`` routes workload signals over HTTP: heartbeats via
  ``transport.heartbeat`` (``POST /internal/v1/runtimes/{runtime_id}/status``) and
  lifecycle signals via ``transport.lifecycle_signal`` (``POST .../lifecycle-signals``).
"""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Mapping

# Serialize writes so heartbeat threads do not interleave lines.
_CONTROL_PLANE_LOG_LOCK = threading.Lock()

BANNER_WR_TO_RM = "-----------------From WR to RM-------------"
BANNER_WR_TO_RM_HTTP = "-----------------From WR to RM (HTTP)-------------"


def write_control_plane_envelope(*, banner: str, message: Mapping[str, Any]) -> None:
    """Print one event-envelope-shaped JSON line to stdout (command window)."""
    now_text = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload_line = json.dumps(
        message, separators=(",", ":"), ensure_ascii=True, default=str
    )
    _block = f"{banner}\n{now_text}: {payload_line}\n"
    with _CONTROL_PLANE_LOG_LOCK:
        sys.stdout.write(_block)
        sys.stdout.flush()
