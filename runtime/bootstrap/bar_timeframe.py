"""Normalize strategy bundle ``bar_timeframe`` / ``replay_bar_timeframe`` labels."""

from __future__ import annotations


def normalize_bar_timeframe_label(raw: str) -> str:
    """Lowercase canonical form for comparisons (e.g. ``1min`` → ``1m``)."""
    t = raw.strip().lower()
    aliases = {
        "1min": "1m",
        "60s": "1m",
        "60sec": "1m",
        "m1": "1m",
    }
    return aliases.get(t, t)


def redis_md_stream_am_supports_timeframe(bar_timeframe: str) -> bool:
    """True when ``bar_timeframe`` matches the 1m aggregate stream ``md:stream:am``."""
    return normalize_bar_timeframe_label(bar_timeframe) == "1m"
