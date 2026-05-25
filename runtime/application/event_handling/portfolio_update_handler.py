from __future__ import annotations

from runtime.application.runtime_state.account_context_updater import (
    AccountContextUpdater,
)
from runtime.domain.model.normalized_events import PortfolioUpdatedEvent


class PortfolioUpdateHandler:
    """
    Applies :class:`PortfolioUpdatedEvent` to runtime account/portfolio context.

    Does not call user strategy hooks.
    """

    def __init__(self, *, context_updater: AccountContextUpdater | None = None) -> None:
        self._context_updater = context_updater

    def handle(self, event: PortfolioUpdatedEvent) -> None:
        if self._context_updater is None:
            return
        self._context_updater.apply_portfolio_update(event)
