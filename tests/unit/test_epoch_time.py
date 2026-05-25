from __future__ import annotations

from datetime import datetime, timezone

from runtime.infrastructure.clock.epoch_time import (
    utc_datetime_to_epoch_millis,
    utc_from_epoch_millis,
    utc_from_epoch_seconds,
)


def test_utc_from_epoch_millis_avoids_fromtimestamp_windows_limits() -> None:
    """Windows ``fromtimestamp(1e12)`` raises OSError(22); epoch+timedelta does not."""
    ms = 1_577_955_600_000  # ~2020-01-02 UTC
    dt = utc_from_epoch_millis(ms)
    assert dt.tzinfo is not None
    assert dt.year == 2020


def test_utc_from_epoch_seconds_fractional() -> None:
    dt = utc_from_epoch_seconds(1.5)
    assert dt == datetime(1970, 1, 1, 0, 0, 1, 500000, tzinfo=timezone.utc)


def test_round_trip_epoch_millis() -> None:
    original = datetime(2024, 6, 15, 12, 30, tzinfo=timezone.utc)
    ms = utc_datetime_to_epoch_millis(original)
    back = utc_from_epoch_millis(ms)
    assert back.replace(microsecond=0) == original.replace(microsecond=0)
