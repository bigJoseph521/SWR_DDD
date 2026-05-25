"""UTC instants without ``datetime.fromtimestamp`` (Windows EINVAL for huge epoch values)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_from_epoch_millis(ts_ms: int) -> datetime:
    """Convert Unix epoch milliseconds to aware UTC (avoids Windows ``fromtimestamp`` limits)."""
    return UTC_EPOCH + timedelta(milliseconds=ts_ms)


def utc_from_epoch_seconds(sec: float) -> datetime:
    """Convert Unix epoch seconds (fractional) to aware UTC."""
    return UTC_EPOCH + timedelta(seconds=sec)


def utc_datetime_to_epoch_millis(dt: datetime) -> int:
    """Milliseconds since Unix epoch for an aware (or naive-as-UTC) datetime."""
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return int((aware.astimezone(timezone.utc) - UTC_EPOCH).total_seconds() * 1000)
