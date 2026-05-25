from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class OrderIntentResult:
    ok: bool
    response: Mapping[str, Any] | None = None
    error_code: str | None = None
    reason_code: str | None = None
