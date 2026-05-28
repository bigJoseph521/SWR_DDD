from __future__ import annotations

import time

from runtime.application.lifecycle.lifecycle_service import LifecycleService
from runtime.domain.enums import WorkerPhase


class WorkerApp:
    def __init__(self, lifecycle: LifecycleService) -> None:
        self._lifecycle = lifecycle

    def start(self) -> None:
        self._lifecycle.start()

    def stop(
        self,
        *,
        emit_termination_signal: bool = True,
        suppress_stopping_phase_stdout: bool = False,
        termination_reason_code: str | None = None,
        termination_message: str | None = None,
    ) -> bool:
        return self._lifecycle.stop(
            emit_termination_signal=emit_termination_signal,
            suppress_stopping_phase_stdout=suppress_stopping_phase_stdout,
            termination_reason_code=termination_reason_code,
            termination_message=termination_message,
        )

    def run(self) -> None:
        startup_failed = False
        try:
            self._lifecycle.run()
        except Exception:
            startup_failed = True
        waiting_reason = "AWAIT_MANAGER_CONTROL"
        if startup_failed:
            waiting_reason = "BOOTSTRAP_FAILED_AWAIT_MANAGER_CONTROL"
        runtime_meta = self._lifecycle.runtime_metadata
        from runtime.infrastructure.observability.stdout_event import write_stdout_event

        write_stdout_event(
            level="INFO",
            event_name="worker.state.waiting",
            message="Waiting for market data",
            runtime_id=str(runtime_meta.get("runtime_id") or "") or None,
            reason_code=waiting_reason,
            local_phase=str(runtime_meta.get("local_phase") or "") or None,
        )
        try:
            if not startup_failed:
                while (
                    self._lifecycle.phase in (WorkerPhase.READY, WorkerPhase.RUNNING)
                    and not self._lifecycle.first_data_received
                ):
                    if self._lifecycle.should_stop_for_completed_backtest_job():
                        self._lifecycle.stop(
                            emit_termination_signal=True,
                            termination_reason_code="RUNTIME_JOB_COMPLETED",
                            termination_message=None,
                        )
                        return
                    write_stdout_event(
                        level="INFO",
                        event_name="worker.market_data.waiting",
                        message="Waiting for data",
                    )
                    time.sleep(60.0)
            while self._lifecycle.phase in (
                WorkerPhase.READY,
                WorkerPhase.RUNNING,
                WorkerPhase.FAILED,
            ):
                if self._lifecycle.should_stop_for_completed_backtest_job():
                    self._lifecycle.stop(
                        emit_termination_signal=True,
                        termination_reason_code="RUNTIME_JOB_COMPLETED",
                        termination_message=None,
                    )
                    break
                time.sleep(0.5)
        except KeyboardInterrupt:
            if self._lifecycle.phase not in (
                WorkerPhase.STOPPED,
                WorkerPhase.COMPLETED,
            ):
                self._lifecycle.stop(
                    emit_termination_signal=True,
                    termination_reason_code="MANUAL_STOP_REQUESTED",
                    termination_message="Interrupted by operator (Ctrl+C).",
                )
