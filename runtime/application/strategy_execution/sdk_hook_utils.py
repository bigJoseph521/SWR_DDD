from __future__ import annotations

from alphovex_sdk.strategy.base import Strategy as SdkStrategy


def replay_sdk_hook_overridden(strategy: object, name: str) -> bool:
    """True if the concrete strategy class defines its own handler (not the SDK default stub)."""
    cls = type(strategy)
    impl = getattr(cls, name, None)
    if not callable(impl):
        return False
    if isinstance(strategy, SdkStrategy):
        base_impl = getattr(SdkStrategy, name, None)
        if base_impl is not None and impl is base_impl:
            return False
    return True
