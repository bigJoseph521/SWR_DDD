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


def test_domain_platform_trace_spec_does_not_import_outer_layers() -> None:
    from pathlib import Path

    domain_root = Path(__file__).resolve().parents[2] / "runtime" / "domain"
    forbidden = ("runtime.bootstrap", "runtime.infrastructure", "runtime.application")
    violations: list[str] = []
    for path in sorted(domain_root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if not (stripped.startswith("from ") or stripped.startswith("import ")):
                continue
            for prefix in forbidden:
                if prefix in stripped:
                    violations.append(
                        f"{path.relative_to(domain_root.parent)}:{line_no}: {stripped}"
                    )
    assert violations == []
