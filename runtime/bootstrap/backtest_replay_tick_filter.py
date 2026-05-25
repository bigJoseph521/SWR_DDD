"""BACKTEST replay ingress: only bar ticks in the configured ``bar_timeframe``."""

from __future__ import annotations

from typing import Any, Mapping

from runtime.bootstrap.bar_timeframe import normalize_bar_timeframe_label


def skip_backtest_replay_tick_for_ingest(
    tick_payload: Mapping[str, Any],
    *,
    expected_bar_timeframe: str,
) -> bool:
    """
    Return True if this replay tick must not be passed to the strategy (non-bar or wrong bar TF).
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
