"""Map stdio ``PORTFOLIO_SNAPSHOT`` wire JSON into in-memory portfolio state."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from runtime.infrastructure.strategy_loader.runtime_stub_support import (
    CashBalance,
    Exposure,
    MarginState,
    PnL,
    PortfolioSnapshot,
)


class PortfolioWireError(ValueError):
    """Invalid runner portfolio snapshot payload."""


def validate_portfolio_wire(portfolio: Mapping[str, Any]) -> None:
    if not isinstance(portfolio, Mapping):
        raise PortfolioWireError("portfolio must be a JSON object")
    if not portfolio:
        raise PortfolioWireError("portfolio must not be empty")
    has_balance = any(
        key in portfolio
        for key in ("balance", "cash_balance", "buying_power", "equity", "positions")
    )
    if not has_balance:
        raise PortfolioWireError(
            "portfolio must include balance and/or positions fields"
        )


def validate_open_orders_wire(orders: list[Any]) -> None:
    if not isinstance(orders, list):
        raise PortfolioWireError("orders must be a JSON array")


def default_runner_portfolio_wire(
    *,
    currency: str = "USD",
    cash_balance: str = "100000",
) -> dict[str, Any]:
    cash = str(cash_balance).strip() or "0"
    cur = str(currency).strip() or "USD"
    return {
        "balance": {
            "currency": cur,
            "cash_balance": cash,
            "buying_power": cash,
            "equity": cash,
        },
        "positions": [],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def portfolio_wire_to_snapshot(
    portfolio: Mapping[str, Any],
    *,
    account_id: str,
    run_id: str,
    default_currency: str = "USD",
) -> PortfolioSnapshot:
    validate_portfolio_wire(portfolio)

    ts = _parse_timestamp(
        portfolio.get("timestamp") or portfolio.get("ts_event") or portfolio.get("ts")
    )
    currency, free, buying_power, equity = _parse_balance_fields(
        portfolio, default_currency
    )

    positions_raw = portfolio.get("positions")
    positions: dict[str, Any] = {}
    if isinstance(positions_raw, dict):
        positions = dict(positions_raw)
    elif isinstance(positions_raw, list):
        for idx, item in enumerate(positions_raw):
            if not isinstance(item, Mapping):
                continue
            key = str(
                item.get("symbol")
                or item.get("instrument_id")
                or item.get("id")
                or f"position_{idx}"
            ).strip()
            if key:
                positions[key] = dict(item)

    return PortfolioSnapshot(
        account_id=account_id,
        run_id=run_id,
        ts_event=ts,
        cash_balance=CashBalance(
            currency=currency,
            free=free,
            locked=_parse_float(portfolio.get("locked"), default=0.0),
            buying_power=buying_power,
            equity=equity,
        ),
        pnl=PnL(),
        exposure=Exposure(),
        margin_state=MarginState(),
        positions=positions,
    )


def _parse_balance_fields(
    portfolio: Mapping[str, Any], default_currency: str
) -> tuple[str, float, float, float]:
    balance = portfolio.get("balance")
    if isinstance(balance, Mapping):
        currency = str(balance.get("currency") or default_currency).strip().upper()
        free = _parse_float(
            balance.get("cash_balance") or balance.get("free"), default=0.0
        )
        buying_power = _parse_float(balance.get("buying_power"), default=free)
        equity = _parse_float(balance.get("equity"), default=free)
        return currency or default_currency, free, buying_power, equity

    currency = str(portfolio.get("currency") or default_currency).strip().upper()
    free = _parse_float(
        portfolio.get("cash_balance") or portfolio.get("free"), default=0.0
    )
    buying_power = _parse_float(portfolio.get("buying_power"), default=free)
    equity = _parse_float(portfolio.get("equity"), default=free)
    return currency or default_currency, free, buying_power, equity


def _parse_float(value: object, *, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    if not text:
        return default
    return float(text)


def _parse_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise PortfolioWireError(f"invalid portfolio timestamp: {value!r}") from exc
        return (
            parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        )
    return datetime.now(timezone.utc)
