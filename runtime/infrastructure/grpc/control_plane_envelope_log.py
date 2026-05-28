"""Backward-compatible import path; prefer ``stdout_event.write_stdout_event``."""

from __future__ import annotations

from runtime.infrastructure.observability.stdout_event import write_stdout_event

__all__ = ["write_stdout_event"]
