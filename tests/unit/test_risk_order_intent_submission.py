from __future__ import annotations

import ast
from pathlib import Path

import grpc
import pytest
from runtime.infrastructure.grpc.risk_order_intent_client import (
    RiskOrderIntentGrpcClient,
    build_risk_order_intent_grpc_client,
)
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.bootstrap.runtime_dependencies_wiring import build_runtime_dependencies
from runtime.domain.policies.mode_policy import Capability, get_mode_policy
from runtime.domain.enums import WorkerMode


def test_build_risk_order_intent_grpc_client_uses_order_intent_service_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeStub:
        def SubmitOrderIntent(self, request, timeout=None):
            return object()

    def _fake_channel(target: str):
        captured["target"] = target
        return object()

    monkeypatch.setattr(grpc, "insecure_channel", _fake_channel)
    monkeypatch.setattr(
        "runtime.infrastructure.grpc.risk_order_intent_client.risk_worker_pb2_grpc.OrderIntentServiceStub",
        lambda _ch: _FakeStub(),
    )

    client = build_risk_order_intent_grpc_client(
        target="127.0.0.1:50099", timeout_seconds=2.5
    )
    assert client is not None
    assert isinstance(client, RiskOrderIntentGrpcClient)
    assert captured["target"] == "127.0.0.1:50099"


def test_runtime_dependencies_use_risk_order_intent_gateway_not_oms_class() -> None:
    class _RiskClient:
        def submit_order_intent(self, payload: dict[str, object]) -> dict[str, object]:
            return {"accepted": True}

    deps = build_runtime_dependencies(
        WorkerMode.PAPER,
        manager_client=object(),
        runtime_identity=object(),
        risk_order_intent_client=_RiskClient(),
    )
    assert deps.risk_order_intent is not None
    assert isinstance(deps.risk_order_intent, RiskOrderIntentGateway)
    assert type(deps.risk_order_intent).__name__ == "RiskOrderIntentGateway"


def test_application_order_intents_do_not_import_infrastructure() -> None:
    repo = Path(__file__).resolve().parents[2]
    order_intents = repo / "runtime" / "application" / "order_intents"
    forbidden_prefix = "runtime.infrastructure"
    violations: list[str] = []
    for path in sorted(order_intents.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if forbidden_prefix in stripped and (
                stripped.startswith("from ") or stripped.startswith("import ")
            ):
                violations.append(f"{path.relative_to(repo)}:{line_no}: {stripped}")
    assert violations == []


def test_application_runtime_dependencies_do_not_import_infrastructure() -> None:
    repo = Path(__file__).resolve().parents[2]
    path = repo / "runtime" / "application" / "runtime_dependencies.py"
    text = path.read_text(encoding="utf-8")
    assert "runtime.infrastructure" not in text


def test_application_bootstrap_paths_do_not_import_oms_gateway() -> None:
    repo = Path(__file__).resolve().parents[2]
    targets = [
        repo / "runtime" / "application",
        repo / "runtime" / "runtime",
        repo / "runtime" / "main.py",
    ]
    forbidden = (
        "from runtime.infrastructure.grpc.risk_order_intent_gateway import OmsGateway",
        "import runtime.infrastructure.grpc.risk_order_intent_gateway",
    )
    violations: list[str] = []
    for base in targets:
        paths = [base] if base.is_file() else base.rglob("*.py")
        for path in paths:
            text = path.read_text(encoding="utf-8")
            for needle in forbidden:
                if needle in text:
                    violations.append(f"{path}: {needle}")
    assert violations == []


def test_main_module_does_not_reference_build_oms_grpc_client() -> None:
    from runtime.bootstrap import runtime_entrypoint
    from runtime.interface import cli as cli_pkg

    cli_source = ast.parse(
        Path(cli_pkg.main.__file__).read_text(encoding="utf-8")  # type: ignore[attr-defined]
    )
    cli_names = {
        node.id for node in ast.walk(cli_source) if isinstance(node, ast.Name)
    }
    assert "build_oms_grpc_client" not in cli_names
    assert "run_runtime_from_cli" in cli_names

    entry_source = ast.parse(
        Path(runtime_entrypoint.__file__).read_text(encoding="utf-8")
    )
    entry_names = {
        node.id for node in ast.walk(entry_source) if isinstance(node, ast.Name)
    }
    assert "build_oms_grpc_client" not in entry_names
    assert "build_runtime_outbound_clients" in entry_names


def test_mode_policy_exposes_risk_order_intent_egress_capability() -> None:
    policy = get_mode_policy(WorkerMode.LIVE)
    assert Capability.RISK_ORDER_INTENT_EGRESS in policy.allowed


def test_risk_gateway_submission_adapter_missing_correlation_fails_closed() -> None:
    from decimal import Decimal

    from runtime.application.order_intents.submit_order_intent import SubmitOrderIntent
    from runtime.domain.launch_spec import LaunchSpec
    from runtime.domain.enums import OrderIntentSide, OrderIntentType
    from runtime.domain.errors import ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID
    from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
    from runtime.infrastructure.grpc.risk_gateway_submission_adapter import (
        RiskGatewaySubmissionAdapter,
    )
    from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
        OrderSubmissionContext,
    )

    class _RiskClient:
        def submit_order_intent(self, payload: dict[str, object]) -> dict[str, object]:
            return {"accepted": True}

    launch_spec = LaunchSpec.from_payload(
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
    adapter = RiskGatewaySubmissionAdapter(
        RiskOrderIntentGateway(get_mode_policy(WorkerMode.PAPER), _RiskClient()),
        submission_context=OrderSubmissionContext(
            launch_spec=launch_spec,
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    use_case = SubmitOrderIntent(submission_port=adapter)
    outcome = use_case.execute(
        StrategyOrderIntent(
            instrument_id="AAPL",
            side=OrderIntentSide.BUY,
            order_type=OrderIntentType.MARKET,
            quantity=Decimal("1"),
            client_order_id="cid-1",
        )
    )

    assert outcome.ok is False
    assert outcome.error_code == "order_intent_wire_mapping_failed"
    assert outcome.reason_code == ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID
    assert adapter.last_wire_payload is None


def test_risk_gateway_submission_adapter_maps_dependency_unavailable() -> None:
    from decimal import Decimal

    from runtime.application.order_intents.submit_order_intent import SubmitOrderIntent
    from runtime.domain.launch_spec import LaunchSpec
    from runtime.domain.enums import OrderIntentSide, OrderIntentType
    from runtime.domain.errors import RISK_SERVICE_UNAVAILABLE
    from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
    from runtime.infrastructure.grpc.risk_gateway_submission_adapter import (
        RiskGatewaySubmissionAdapter,
    )
    from runtime.infrastructure.grpc.dependency_client_error import DependencyClientError
    from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
        OrderSubmissionContext,
    )

    class _UnavailableClient:
        def submit_order_intent(self, payload: dict[str, object]) -> dict[str, object]:
            raise DependencyClientError(
                code="DEPENDENCY_UNAVAILABLE",
                message="Risk Service dependency is unavailable.",
                retryable=True,
                details={"grpc_code": "StatusCode.UNAVAILABLE"},
            )

    launch_spec = LaunchSpec.from_payload(
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
            "correlation_id": "corr-1",
        }
    )
    adapter = RiskGatewaySubmissionAdapter(
        RiskOrderIntentGateway(get_mode_policy(WorkerMode.PAPER), _UnavailableClient()),
        submission_context=OrderSubmissionContext(
            launch_spec=launch_spec,
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    outcome = SubmitOrderIntent(submission_port=adapter).execute(
        StrategyOrderIntent(
            instrument_id="AAPL",
            side=OrderIntentSide.BUY,
            order_type=OrderIntentType.MARKET,
            quantity=Decimal("1"),
            client_order_id="cid-1",
        )
    )

    assert outcome.ok is False
    assert outcome.reason_code == RISK_SERVICE_UNAVAILABLE
    assert "Risk Service dependency" not in (outcome.reason_code or "")
    assert adapter.last_wire_payload is not None
    assert adapter.last_wire_payload["correlation_id"] == "corr-1"
