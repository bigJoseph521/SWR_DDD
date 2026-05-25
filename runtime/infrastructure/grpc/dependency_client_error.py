from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class DependencyClientError(Exception):
    """Typed failure from an outbound gRPC dependency client (Risk Service, etc.)."""

    code: str
    message: str
    retryable: bool = False
    details: Mapping[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message
