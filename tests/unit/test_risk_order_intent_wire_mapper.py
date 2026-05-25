from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest
from alphovex_sdk.enums.order import OrderSide, OrderType, TimeInForce
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.domain.enums import OrderIntentSide, OrderIntentType, WorkerMode
from runtime.domain.errors import (
    ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
    OrderIntentWireMappingError,
)
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
    build_risk_order_intent_wire_payload,
    strategy_order_intent_from_sdk,
)
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.infrastructure.strategy_loader.replay_runtime_support import ReplayOrderIntent


def _launch_spec(**extra: object) -> LaunchSpec:
    payload: dict[str, object] = {
        "runtime_id": "rt-1",
        "tenant_id": "t1",
        "strategy_version_id": "sv1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "artifact_uri": "file:///x",
        "artifact_digest": "sha256:aa",
        "entrypoint": "m:s",
        "account_id": "acct-1",
        "validated_parameter_identity": "vp-1",
        "correlation_id": "corr-default",
    }
    payload.update(extra)
    return LaunchSpec.from_payload(payload)


def _intent(**extra: object) -> StrategyOrderIntent:
    base = {
        "instrument_id": "AAPL",
        "side": OrderIntentSide.BUY,
        "order_type": OrderIntentType.MARKET,
        "quantity": Decimal("1"),
        "client_order_id": "cid-1",
    }
    base.update(extra)
    return StrategyOrderIntent(**base)  # type: ignore[arg-type]


def test_build_risk_wire_payload_includes_symbol_and_job_id() -> None:
    spec = _launch_spec(symbol="MSFT", job_id="job-77")
    wire = build_risk_order_intent_wire_payload(
        _intent(),
        context=OrderSubmissionContext(
            launch_spec=spec,
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    assert wire["symbol"] == "MSFT"
    assert wire["job_id"] == "job-77"
    assert wire["runtime_id"] == "rt-1"
    assert wire["strategy_version_id"] == "sv1"
    assert wire["mode"] == "PAPER"


def test_build_risk_wire_payload_prefers_launch_spec_correlation_id() -> None:
    spec = _launch_spec(correlation_id="corr-bundle", symbol="AAPL")
    wire = build_risk_order_intent_wire_payload(
        _intent(),
        context=OrderSubmissionContext(
            launch_spec=spec,
            correlation_id_fallback="corr-fallback",
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    assert wire["correlation_id"] == "corr-bundle"


def test_build_risk_wire_payload_uses_platform_trace_correlation_when_bundle_absent() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
        }
    )
    trace = PlatformTraceSpec(
        strategy_id="strat-1",
        strategy_version_id="sv1",
        correlation_id="corr-trace",
        request_id="rt-1:1",
    )
    wire = build_risk_order_intent_wire_payload(
        _intent(),
        context=OrderSubmissionContext(
            launch_spec=spec,
            platform_trace=trace,
            correlation_id_fallback="corr-fallback",
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    assert wire["correlation_id"] == "corr-trace"


def test_build_risk_wire_payload_correlation_id_falls_back_to_runtime_fallback() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
        }
    )
    wire = build_risk_order_intent_wire_payload(
        _intent(),
        context=OrderSubmissionContext(
            launch_spec=spec,
            correlation_id_fallback="corr-env-only",
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    assert wire["correlation_id"] == "corr-env-only"


def test_build_risk_wire_payload_uses_platform_trace_env_correlation_fallback() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
        }
    )
    trace = PlatformTraceSpec(
        strategy_id=None,
        strategy_version_id="sv1",
        correlation_id=None,
        request_id="rt-1:1",
    )
    wire = build_risk_order_intent_wire_payload(
        _intent(),
        context=OrderSubmissionContext(
            launch_spec=spec,
            correlation_id_fallback="corr-env-only",
            platform_trace=trace,
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    assert wire["correlation_id"] == "corr-env-only"


def test_build_risk_wire_payload_missing_correlation_raises_typed_error() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
            "job_id": "job-1",
        }
    )
    with pytest.raises(OrderIntentWireMappingError) as exc_info:
        build_risk_order_intent_wire_payload(
            _intent(),
            context=OrderSubmissionContext(
                launch_spec=spec,
                allocate_order_intent_id=lambda: "oi-1",
            ),
        )
    assert exc_info.value.reason_code == ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID
    assert exc_info.value.diagnostics["runtime_id"] == "rt-1"
    assert exc_info.value.diagnostics["strategy_version_id"] == "sv1"
    assert exc_info.value.diagnostics["account_id"] == "acct-1"
    assert exc_info.value.diagnostics["job_id"] == "job-1"


def test_build_risk_wire_payload_does_not_use_uuid_for_missing_correlation() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
        }
    )
    with patch(
        "runtime.infrastructure.grpc.risk_order_intent_wire_mapper.uuid.uuid4"
    ) as mock_uuid:
        with pytest.raises(OrderIntentWireMappingError):
            build_risk_order_intent_wire_payload(
                _intent(),
                context=OrderSubmissionContext(
                    launch_spec=spec,
                    allocate_order_intent_id=lambda: "oi-1",
                ),
            )
        mock_uuid.assert_not_called()


def test_strategy_order_intent_from_sdk_replay_intent() -> None:
    sdk = ReplayOrderIntent(
        instrument_id=" AAPL ",
        side=OrderSide.BUY,
        quantity=2.0,
        price=1.0,
        order_type=OrderType.LIMIT,
        limit_price=10.5,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
        client_order_id="cid-1",
    )
    domain = strategy_order_intent_from_sdk(sdk)
    assert domain.instrument_id == "AAPL"
    assert domain.side is OrderIntentSide.BUY
    assert domain.order_type is OrderIntentType.LIMIT
    assert domain.quantity == Decimal("2")
    assert domain.limit_price == Decimal("10.5")
    assert domain.client_order_id == "cid-1"
