from __future__ import annotations

from datetime import datetime, timezone

from runtime.bootstrap.replay_runtime_support import (
    CashBalance,
    Exposure,
    MarginState,
    PnL,
    PortfolioSnapshot,
    SnapshotPortfolioService,
)
from runtime.strategy_contract.runtime_account_context import RuntimeAccountContext


def test_apply_pls_balance_update_mutates_snapshot() -> None:
    snap = PortfolioSnapshot(
        account_id="acct-1",
        run_id="run-1",
        ts_event=datetime(2026, 1, 1, tzinfo=timezone.utc),
        cash_balance=CashBalance(currency="USD", free=100.0, locked=10.0),
        pnl=PnL(),
        exposure=Exposure(),
        margin_state=MarginState(),
        positions={},
    )
    svc = SnapshotPortfolioService(snap)
    account = RuntimeAccountContext(portfolio_service=svc)

    applied = account.apply_pls_balance_update(
        {
            "job_id": "ignored-by-account",
            "timestamp": "2026-05-18T15:30:00.000Z",
            "balance": {
                "cash_balance": "500.25",
                "buying_power": "400",
                "equity": "600.5",
            },
        }
    )
    assert applied is True

    portfolio = account.portfolio()
    assert portfolio.balance.cash_balance == 500.25
    assert portfolio.balance.buying_power == 400.0
    assert portfolio.balance.equity == 600.5
    assert portfolio.balance.available_funds == 400.0

    updated = svc.get_snapshot()
    assert updated.cash_balance.free == 500.25
    assert updated.cash_balance.buying_power == 400.0
    assert updated.cash_balance.equity == 600.5
