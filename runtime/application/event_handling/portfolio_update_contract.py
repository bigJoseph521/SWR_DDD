from __future__ import annotations

import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from runtime.domain.model.normalized_events import PortfolioUpdatedEvent

DEFAULT_CHANNEL_PREFIX = "portfolio:update"


@dataclass(frozen=True, slots=True)
class PortfolioUpdateParseResult:
    event: PortfolioUpdatedEvent | None
    reject_reason: str | None = None


def portfolio_update_partition(job_id: str, partition_count: int) -> int:
    """
    ``partition = zlib.crc32(job_id.encode("utf-8")) % partition_count`` (IEEE CRC32).
    """
    if partition_count < 1:
        raise ValueError("partition_count must be >= 1")
    normalized = str(job_id or "").strip()
    crc = zlib.crc32(normalized.encode("utf-8")) & 0xFFFFFFFF
    return int(crc % partition_count)


def portfolio_update_channel_name(
    channel_prefix: str, *, partition: int
) -> str:
    base = str(channel_prefix or DEFAULT_CHANNEL_PREFIX).strip() or DEFAULT_CHANNEL_PREFIX
    return f"{base.rstrip(':')}:{int(partition)}"


def parse_decimal_balance_value(raw: object, *, field_name: str) -> Decimal | None:
    """Parse PLS balance fields as :class:`Decimal` (never ``float``)."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, int):
        return Decimal(raw)
    if isinstance(raw, float):
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _parse_timestamp(raw: object) -> datetime | None:
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


def parse_portfolio_update_message(
    payload: Mapping[str, Any],
    *,
    expected_job_id: str,
) -> PortfolioUpdateParseResult:
    """
    Validate PLS wire JSON and build :class:`PortfolioUpdatedEvent`.

    Messages whose ``job_id`` does not match ``expected_job_id`` are ignored (not rejected).
    """
    expected = str(expected_job_id or "").strip()
    if not expected:
        return PortfolioUpdateParseResult(
            event=None, reject_reason="expected_job_id_missing"
        )

    job_id_raw = payload.get("job_id")
    if not isinstance(job_id_raw, str) or not job_id_raw.strip():
        return PortfolioUpdateParseResult(
            event=None, reject_reason="job_id_required"
        )
    job_id = job_id_raw.strip()
    if job_id != expected:
        return PortfolioUpdateParseResult(event=None, reject_reason=None)

    timestamp = _parse_timestamp(payload.get("timestamp"))
    if timestamp is None:
        return PortfolioUpdateParseResult(
            event=None, reject_reason="timestamp_required"
        )

    balance = payload.get("balance")
    if not isinstance(balance, Mapping):
        return PortfolioUpdateParseResult(
            event=None, reject_reason="balance_required"
        )

    cash = parse_decimal_balance_value(
        balance.get("cash_balance"), field_name="cash_balance"
    )
    buying_power = parse_decimal_balance_value(
        balance.get("buying_power"), field_name="buying_power"
    )
    equity = parse_decimal_balance_value(balance.get("equity"), field_name="equity")

    if cash is None:
        return PortfolioUpdateParseResult(
            event=None, reject_reason="balance.cash_balance_required"
        )
    if buying_power is None:
        return PortfolioUpdateParseResult(
            event=None, reject_reason="balance.buying_power_required"
        )
    if equity is None:
        return PortfolioUpdateParseResult(
            event=None, reject_reason="balance.equity_required"
        )

    return PortfolioUpdateParseResult(
        event=PortfolioUpdatedEvent(
            job_id=job_id,
            timestamp=timestamp,
            cash_balance=cash,
            buying_power=buying_power,
            equity=equity,
        )
    )
