from __future__ import annotations

from runtime.domain.model.platform_trace_spec import PlatformTraceSpec


def test_effective_correlation_id_prefers_bundle_over_env() -> None:
    trace = PlatformTraceSpec(
        strategy_id="s1",
        strategy_version_id="sv-1",
        correlation_id="corr-bundle",
        request_id="req-1",
    )
    assert trace.effective_correlation_id(env_fallback="corr-env") == "corr-bundle"


def test_effective_correlation_id_falls_back_to_env() -> None:
    trace = PlatformTraceSpec(
        strategy_id=None,
        strategy_version_id="sv-1",
        correlation_id=None,
        request_id="req-1",
    )
    assert trace.effective_correlation_id(env_fallback="corr-env") == "corr-env"


def test_merge_event_extras_does_not_overwrite_explicit_keys() -> None:
    trace = PlatformTraceSpec(
        strategy_id="s1",
        strategy_version_id="sv-1",
        correlation_id="corr-bundle",
        request_id="req-1",
    )
    merged = trace.merge_event_extras(
        {"correlation_id": "explicit", "symbol": "AAPL"},
        env_correlation_fallback="corr-env",
    )
    assert merged["correlation_id"] == "explicit"
    assert merged["strategy_id"] == "s1"
    assert merged["request_id"] == "req-1"
    assert merged["symbol"] == "AAPL"
