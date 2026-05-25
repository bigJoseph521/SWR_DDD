from __future__ import annotations

import threading
from datetime import datetime

from runtime.domain.enums import WorkerPhase


class RuntimeState:
    """Worker phase and last-ingested data event timestamp."""

    def __init__(self, *, initial_phase: WorkerPhase = WorkerPhase.INITIALIZING) -> None:
        self._lock = threading.Lock()
        self._phase = initial_phase
        self._last_data_event_timestamp: datetime | None = None
        self._last_replay_cursor: str = ""
        self._backtest_replay_complete = False
        self._first_data_received = False

    @property
    def phase(self) -> WorkerPhase:
        with self._lock:
            return self._phase

    def set_phase(self, phase: WorkerPhase) -> None:
        with self._lock:
            self._phase = phase

    def accepts_work(self) -> bool:
        with self._lock:
            return self._phase in (WorkerPhase.READY, WorkerPhase.RUNNING)

    @property
    def last_data_event_timestamp(self) -> datetime | None:
        with self._lock:
            return self._last_data_event_timestamp

    def record_data_event_timestamp(self, ts: datetime | None) -> None:
        if ts is None:
            return
        with self._lock:
            self._last_data_event_timestamp = ts

    @property
    def last_replay_cursor(self) -> str:
        with self._lock:
            return self._last_replay_cursor

    def set_last_replay_cursor(self, cursor: str) -> None:
        with self._lock:
            self._last_replay_cursor = cursor

    @property
    def backtest_replay_complete(self) -> bool:
        with self._lock:
            return self._backtest_replay_complete

    def mark_backtest_replay_complete(self) -> None:
        with self._lock:
            self._backtest_replay_complete = True

    @property
    def first_data_received(self) -> bool:
        with self._lock:
            return self._first_data_received

    def mark_first_data_received(self) -> None:
        with self._lock:
            self._first_data_received = True
