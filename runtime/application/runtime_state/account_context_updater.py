from __future__ import annotations

from typing import Protocol

from runtime.domain.model.normalized_events import PortfolioUpdatedEvent


class AccountContextUpdater(Protocol):
    """Updates runtime account/portfolio context from portfolio events."""

    def apply_portfolio_update(self, event: PortfolioUpdatedEvent) -> None: ...
