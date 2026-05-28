"""Cooperative worker shutdown from HTTP stop and Kubernetes SIGTERM."""

from __future__ import annotations

import logging
import signal
import threading
from typing import Protocol

from runtime.application.lifecycle.stop_errors import (
    StopAlreadyInProgress,
    WorkerAlreadyStopped,
)
from runtime.domain.errors import RuntimeWorkerReasonCode

KUBERNETES_TERMINATION_REASON = RuntimeWorkerReasonCode.KUBERNETES_TERMINATION.value

_LOG = logging.getLogger(__name__)


class _LifecycleStopInitiator(Protocol):
    def initiate_stop_from_request(self, reason: str) -> dict[str, object]: ...


class _WorkerStopRunner(Protocol):
    def stop(
        self,
        *,
        emit_termination_signal: bool = True,
        suppress_stopping_phase_stdout: bool = False,
        termination_reason_code: str | None = None,
        termination_message: str | None = None,
    ) -> bool: ...


class CooperativeShutdownCoordinator:
    """
    Single idempotent entry for cooperative shutdown.

    HTTP ``POST /internal/v1/stop`` and Kubernetes SIGTERM both use this coordinator
    so only one background ``LifecycleService.stop`` runs.
    """

    def __init__(
        self,
        *,
        lifecycle: _LifecycleStopInitiator,
        worker_app: _WorkerStopRunner,
    ) -> None:
        self._lifecycle = lifecycle
        self._worker_app = worker_app
        self._thread_lock = threading.Lock()
        self._shutdown_thread_started = False

    def request_http_stop(self, reason: str) -> dict[str, object]:
        """Initiate shutdown for manager HTTP stop; propagates stop conflicts to HTTP 409."""
        body = self._lifecycle.initiate_stop_from_request(reason)
        self._ensure_shutdown_thread()
        return body

    def request_sigterm_shutdown(self) -> None:
        """Initiate shutdown for Kubernetes SIGTERM; never raises to the signal handler."""
        try:
            self._lifecycle.initiate_stop_from_request(KUBERNETES_TERMINATION_REASON)
        except StopAlreadyInProgress:
            _LOG.info(
                "kubernetes_sigterm_ignored reason=%s",
                "shutdown_already_in_progress",
            )
        except WorkerAlreadyStopped:
            _LOG.info(
                "kubernetes_sigterm_ignored reason=%s",
                "worker_already_stopped",
            )
            return
        except Exception:
            _LOG.error("kubernetes_sigterm_initiate_failed", exc_info=False)
        self._ensure_shutdown_thread()

    def _ensure_shutdown_thread(self) -> None:
        with self._thread_lock:
            if self._shutdown_thread_started:
                return
            self._shutdown_thread_started = True
        threading.Thread(
            target=self._run_shutdown,
            name="swr-cooperative-shutdown",
            daemon=True,
        ).start()

    def _run_shutdown(self) -> None:
        try:
            self._worker_app.stop(
                emit_termination_signal=False,
                suppress_stopping_phase_stdout=True,
            )
        except Exception:
            _LOG.error("cooperative_shutdown_worker_stop_failed", exc_info=False)


def install_sigterm_handler(coordinator: CooperativeShutdownCoordinator) -> bool:
    """
    Install a process SIGTERM handler when the platform supports it.

    Returns True when a handler was registered.
    """
    if not hasattr(signal, "SIGTERM"):
        return False

    handler_lock = threading.Lock()
    handler_invoked = False

    def _on_sigterm(signum: int, frame: object | None) -> None:
        del signum, frame
        nonlocal handler_invoked
        with handler_lock:
            if handler_invoked:
                return
            handler_invoked = True
        _LOG.info("kubernetes_sigterm_received")
        try:
            coordinator.request_sigterm_shutdown()
        except Exception:
            _LOG.error("kubernetes_sigterm_handler_failed", exc_info=False)

    signal.signal(signal.SIGTERM, _on_sigterm)
    return True
