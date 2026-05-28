"""Subprocess session lifecycle states."""

from __future__ import annotations

from enum import StrEnum


class SessionState(StrEnum):
    CREATED = "CREATED"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
