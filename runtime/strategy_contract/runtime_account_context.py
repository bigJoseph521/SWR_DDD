from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from alphovex_sdk.context.account_context import AccountContext
from alphovex_sdk.models.account import Balance, Portfolio, Position
from alphovex_sdk.typedefs import Symbol


def _snapshot_to_portfolio(snap: Any) -> Portfolio:
    """Build SDK :class:`Portfolio` from a replay :class:`PortfolioSnapshot`-like object."""
    ts = getattr(snap, "ts_event", None)
    if not isinstance(ts, datetime):
        raise TypeError("snapshot.ts_event must be a datetime")

    cash = getattr(snap, "cash_balance", None)
    free = float(getattr(cash, "free", 0.0) or 0.0) if cash is not None else 0.0
    bp_raw = getattr(cash, "buying_power", None) if cash is not None else None
    eq_raw = getattr(cash, "equity", None) if cash is not None else None
    buying_power = float(bp_raw) if bp_raw is not None else free
    equity = float(eq_raw) if eq_raw is not None else free
    available = min(buying_power, equity)

    balance = Balance(
        timestamp=ts,
        cash_balance=free,
        buying_power=buying_power,
        equity=equity,
        initial_margin=0.0,
        maintenance_margin=0.0,
        available_funds=available,
    )

    raw_positions = getattr(snap, "positions", None)
    positions: list[Position] = []
    if isinstance(raw_positions, dict):
        for _k, pos in raw_positions.items():
            if isinstance(pos, Position):
                positions.append(pos)
            elif pos is not None:
                sym = str(
                    getattr(pos, "symbol", None)
                    or getattr(pos, "instrument_id", "")
                    or ""
                )
                qty = float(getattr(pos, "quantity", 0.0) or 0.0)
                avg = float(getattr(pos, "average_price", 0.0) or 0.0)
                mkt = float(getattr(pos, "market_price", avg) or avg)
                mv = float(getattr(pos, "market_value", qty * mkt) or (qty * mkt))
                upnl = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
                p_ts = getattr(pos, "timestamp", None) or ts
                if not isinstance(p_ts, datetime):
                    p_ts = ts
                if sym:
                    positions.append(
                        Position(
                            symbol=sym,
                            quantity=qty,
                            average_price=avg,
                            market_price=mkt,
                            market_value=mv,
                            unrealized_pnl=upnl,
                            timestamp=p_ts,
                        )
                    )

    return Portfolio(balance=balance, timestamp=ts, positions=positions)


class RuntimeAccountContext(AccountContext):
    """Thin :class:`AccountContext` over replay ``SnapshotPortfolioService``."""

    __slots__ = ("_portfolio",)

    def __init__(self, *, portfolio_service: Any) -> None:
        self._portfolio = portfolio_service

    def portfolio(self) -> Portfolio:
        get_snap = getattr(self._portfolio, "get_snapshot", None)
        if callable(get_snap):
            snap = get_snap()
        else:
            snap = getattr(self._portfolio, "_snapshot", None)
            if snap is None:
                snap_attr = getattr(self._portfolio, "snapshot", None)
                if callable(snap_attr):
                    snap = snap_attr()
                else:
                    snap = snap_attr
        if snap is None:
            raise RuntimeError("portfolio service has no accessible snapshot")
        return _snapshot_to_portfolio(snap)

    def apply_pls_balance_update(self, msg: Mapping[str, Any]) -> bool:
        """
        Apply a portfolio-ledger ``portfolio:update`` message to in-memory snapshot state.

        Returns True when ``job_id`` matched and balance fields were applied.
        """
        apply_fn = getattr(self._portfolio, "apply_pls_balance_update", None)
        if not callable(apply_fn):
            return False

        balance = msg.get("balance")
        if not isinstance(balance, Mapping):
            return False

        ts = _parse_pls_timestamp(msg.get("timestamp")) or datetime.now(timezone.utc)
        cash = _parse_cash_value(balance.get("cash_balance"))
        bp = _parse_cash_value(balance.get("buying_power"))
        eq = _parse_cash_value(balance.get("equity"))

        apply_fn(
            cash_balance=cash,
            buying_power=bp,
            equity=eq,
            timestamp=ts,
        )
        return True

    def position(self, symbol: Symbol) -> Position | None:
        return self.portfolio().get_position(symbol)

    def positions(self) -> Sequence[Position]:
        return tuple(self.portfolio().positions)


def _parse_cash_value(raw: object) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if not text:
        return 0.0
    return float(text)


def _parse_pls_timestamp(raw: object) -> datetime | None:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
