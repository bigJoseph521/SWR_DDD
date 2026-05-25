from __future__ import annotations


class StopAlreadyInProgress(RuntimeError):
    """Cooperative shutdown already running."""


class WorkerAlreadyStopped(RuntimeError):
    """Worker lifecycle has already reached a terminal stopped state."""
