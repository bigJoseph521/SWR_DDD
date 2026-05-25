"""Filter BACKTEST historical events by configured bar timeframe (ingress only)."""

from __future__ import annotations

from typing import Any, Mapping

from runtime.domain.bar_timeframe import normalize_bar_timeframe_label


def should_skip_backtest_historical_market_event(
    tick_payload: Mapping[str, Any],
    *,
    expected_bar_timeframe: str,
) -> bool:
    """
    Return True if this historical event must not enter the shared dispatcher (non-bar or wrong TF).
    """
    et = (
        str(tick_payload.get("type") or tick_payload.get("event_type") or "")
        .strip()
        .lower()
    )
    if et != "market.bar":
        return True
    exp = normalize_bar_timeframe_label(expected_bar_timeframe)
    tick_tf_raw = str(tick_payload.get("timeframe") or "").strip()
    if not tick_tf_raw:
        return False
    return normalize_bar_timeframe_label(tick_tf_raw) != exp
