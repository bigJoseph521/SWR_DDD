from __future__ import annotations

from typing import Callable, Mapping, Protocol


class InputFeedPort(Protocol):
    """Mode-specific market/portfolio input feed."""

    def start(self, on_event: Callable[[Mapping[str, object]], None]) -> None: ...

    def stop(self) -> None: ...
