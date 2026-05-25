from __future__ import annotations

from datetime import datetime
from typing import Protocol


class RuntimeJournalPort(Protocol):
    def record_manager_stop_request(
        self,
        *,
        last_data_event_at: datetime | None,
        last_clock_at: datetime | None,
        last_replay_cursor: str,
        stop_requested_at: datetime,
    ) -> None: ...
