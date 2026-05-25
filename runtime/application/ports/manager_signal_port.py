from __future__ import annotations

from typing import Mapping, Protocol


class ManagerSignalPort(Protocol):
    """SRM lifecycle signal emission."""

    def emit_signal(self, payload: Mapping[str, object]) -> Mapping[str, object]: ...
