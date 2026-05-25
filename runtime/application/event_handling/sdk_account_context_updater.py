from __future__ import annotations

from typing import Any

from runtime.domain.model.normalized_events import PortfolioUpdatedEvent


class SdkAccountContextUpdater:
    """
    Updates runtime SDK account/portfolio context from normalized portfolio events.

    Does not invoke user strategy hooks.
    """

    def __init__(self, account_context: Any) -> None:
        self._account = account_context

    def apply_portfolio_update(self, event: PortfolioUpdatedEvent) -> None:
        apply_event = getattr(self._account, "apply_portfolio_balance_event", None)
        if callable(apply_event):
            apply_event(event)
            return
        apply_legacy = getattr(self._account, "apply_pls_balance_update", None)
        if callable(apply_legacy):
            apply_legacy(
                {
                    "job_id": event.job_id,
                    "timestamp": event.timestamp.isoformat(),
                    "balance": {
                        "cash_balance": str(event.cash_balance),
                        "buying_power": str(event.buying_power),
                        "equity": str(event.equity),
                    },
                }
            )
