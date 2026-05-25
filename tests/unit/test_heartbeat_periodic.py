from __future__ import annotations

import time

from runtime.application.heartbeat.heartbeat_periodic import HeartbeatPeriodicJobs


def test_periodic_invokes_tick_repeatedly() -> None:
    calls: list[int] = []
    jobs = HeartbeatPeriodicJobs(on_tick=lambda: calls.append(1), interval_seconds=0.08)
    jobs.start()
    time.sleep(0.22)
    jobs.stop()
    assert len(calls) >= 2
