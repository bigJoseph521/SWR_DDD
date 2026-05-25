import base64
import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path

GENERATED_DIR = Path(__file__).resolve().parents[2] / "protos" / "generated"
if str(GENERATED_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIR))

json_format = importlib.import_module("google.protobuf.json_format")
importlib.import_module(
    "google.protobuf.empty_pb2"
)  # register empty.proto before generated *_pb2
Struct = importlib.import_module("google.protobuf.struct_pb2").Struct
Timestamp = importlib.import_module("google.protobuf.timestamp_pb2").Timestamp

manager_worker_pb2 = importlib.import_module("manager_worker_pb2")
manager_worker_pb2_grpc = importlib.import_module("manager_worker_pb2_grpc")
risk_worker_pb2 = importlib.import_module("risk_worker_pb2")
risk_worker_pb2_grpc = importlib.import_module("risk_worker_pb2_grpc")
replay_worker_pb2 = importlib.import_module("replay_worker_pb2")
replay_worker_pb2_grpc = importlib.import_module("replay_worker_pb2_grpc")


def _ts(value: str):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    out = Timestamp()
    out.FromDatetime(dt)
    return out


def _append_unknown_string_field(
    payload: bytes, field_number: int, value: str
) -> bytes:
    # Unknown field encoding keeps older payloads forward-compatible.
    key = (field_number << 3) | 2

    def _encode_varint(n: int) -> bytes:
        out = bytearray()
        while True:
            to_write = n & 0x7F
            n >>= 7
            if n:
                out.append(to_write | 0x80)
            else:
                out.append(to_write)
                break
        return bytes(out)

    raw = value.encode("utf-8")
    return payload + _encode_varint(key) + _encode_varint(len(raw)) + raw


def _field_numbers(message_cls):
    return {field.name: field.number for field in message_cls.DESCRIPTOR.fields}


def _to_dict(msg):
    return json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=False,
    )


def test_generated_proto_modules_import_cleanly():
    assert manager_worker_pb2.DESCRIPTOR.name == "manager_worker.proto"
    assert replay_worker_pb2.DESCRIPTOR.name == "replay_worker.proto"
    assert risk_worker_pb2.DESCRIPTOR.name == "risk_worker.proto"


def test_service_classes_exist():
    assert hasattr(manager_worker_pb2_grpc, "WorkerControlServiceStub")
    assert hasattr(manager_worker_pb2_grpc, "WorkerLifecycleSignalServiceStub")
    assert hasattr(replay_worker_pb2_grpc, "ReplayIngressServiceStub")
    assert hasattr(replay_worker_pb2_grpc, "ReplayLookbackServiceStub")
    assert hasattr(replay_worker_pb2_grpc, "BacktestOrderIntentServiceStub")
    assert hasattr(risk_worker_pb2_grpc, "OrderIntentServiceStub")


def test_message_classes_exist():
    for module, names in [
        (
            manager_worker_pb2,
            [
                "LaunchSpec",
                "Heartbeat",
                "LaunchSucceeded",
                "LaunchFailed",
                "TerminationReported",
            ],
        ),
        (
            replay_worker_pb2,
            [
                "ReplayContext",
                "ReplayLookbackRequest",
                "ReplayLookbackResponse",
                "BacktestOrderIntent",
            ],
        ),
        (
            risk_worker_pb2,
            [
                "OrderIntent",
                "OrderIntentAck",
                "CancelOrderIntent",
                "ReplaceOrderIntent",
            ],
        ),
    ]:
        for name in names:
            assert hasattr(module, name), f"missing message class {name}"


def test_required_identity_and_time_fields_present_on_key_messages():
    manager_required = {
        "LaunchSpec": {
            "contract",
            "correlation_id",
            "runtime_id",
            "mode",
            "entrypoint",
            "requested_at",
            "job_id",
        },
        "Heartbeat": {"contract", "event_id", "occurred_at", "payload"},
        "LaunchSucceeded": {"contract", "event_id", "occurred_at", "payload"},
        "LaunchFailed": {"contract", "event_id", "occurred_at", "payload"},
        "TerminationReported": {"contract", "event_id", "occurred_at", "payload"},
    }
    for name, expected in manager_required.items():
        fields = set(_field_numbers(getattr(manager_worker_pb2, name)).keys())
        assert expected.issubset(fields), f"{name} missing fields: {expected - fields}"

    replay_required = {
        "ReplayContext": {"replay", "events", "simulated_time", "end_of_stream"},
        "ReplayLookbackRequest": {
            "replay",
            "instrument_id",
            "start_time",
            "end_time",
            "request_purpose",
        },
        "ReplayLookbackResponse": {"replay", "items", "coverage", "served_at"},
        "BacktestOrderIntent": {
            "contract",
            "meta",
            "replay_session_id",
            "requested_at",
            "created_at",
            "order_intent_id",
        },
    }
    for name, expected in replay_required.items():
        fields = set(_field_numbers(getattr(replay_worker_pb2, name)).keys())
        assert expected.issubset(fields), f"{name} missing fields: {expected - fields}"

    oms_required = {
        "OrderIntent": {
            "correlation_id",
            "account_id",
            "mode",
            "idempotency_key",
            "requested_at",
            "created_at",
            "order_intent_id",
            "symbol",
        },
        "OrderIntentAck": {"meta", "status", "accepted", "observed_at"},
        "CancelOrderIntent": {
            "correlation_id",
            "account_id",
            "job_id",
            "target_oms_order_id",
            "idempotency_key",
            "requested_at",
            "extension",
            "mode",
        },
    }
    for name, expected in oms_required.items():
        fields = set(_field_numbers(getattr(risk_worker_pb2, name)).keys())
        assert expected.issubset(fields), f"{name} missing fields: {expected - fields}"


def test_launch_spec_round_trip():
    msg = manager_worker_pb2.LaunchSpec(
        contract=manager_worker_pb2.ContractVersion(
            schema_version=1, compatibility_note="initial freeze"
        ),
        correlation_id="corr-1",
        causation_id="cause-0",
        tenant_id="tenant-a",
        account_id="acct-1",
        runtime_id="rt-123",
        worker_identity="worker-local-1",
        launch_attempt=2,
        strategy_version_id="sv-20260327",
        job_id="job-bt-1",
        deployment_id="dep-1",
        mode=manager_worker_pb2.PAPER,
        trader_id="trader-7",
        validated_parameter_identity="vp-abc",
        parameter_hash="sha256:def",
        artifact_reference="strategy-image:v2",
        artifact_uri="registry.local/strategy:v2",
        artifact_digest="sha256:1234",
        entrypoint="my_strategy.main:run",
        sandbox_profile="restricted",
        requested_by="manager",
        requested_at=_ts("2026-03-27T09:00:00Z"),
    )
    out = manager_worker_pb2.LaunchSpec()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_heartbeat_round_trip():
    hb_payload = Struct()
    hb_payload.update({"local_state": "RUNNING"})
    msg = manager_worker_pb2.Heartbeat(
        contract=manager_worker_pb2.ContractVersion(schema_version=1),
        event_id="evt-hb-1",
        event_name="runtime.heartbeat",
        event_version=1,
        producer="strategy-worker-runtime",
        correlation_id="corr-hb",
        tenant_id="tenant-a",
        account_id="acct-1",
        runtime_id="rt-123",
        worker_identity="worker-local-1",
        launch_attempt=2,
        strategy_version_id="sv-20260327",
        occurred_at=_ts("2026-03-27T09:05:00Z"),
        payload=hb_payload,
    )
    out = manager_worker_pb2.Heartbeat()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_replay_context_round_trip():
    payload = Struct()
    payload.update({"bar": 2})
    msg = replay_worker_pb2.ReplayContext(
        replay=replay_worker_pb2.ReplayMetadata(
            contract=replay_worker_pb2.ContractVersion(schema_version=1),
            meta=replay_worker_pb2.CommonMetadata(
                correlation_id="corr-rp",
                tenant_id="tenant-a",
                runtime_id="rt-123",
                launch_attempt=2,
                strategy_version_id="sv-20260327",
            ),
            replay_session_id="replay-s-1",
            replay_cursor="cursor-1",
            dataset_version="dataset-10",
            policy_snapshot_id="policy-3",
            order_key=replay_worker_pb2.EVENT_TIME_ASC_SEQUENCE_ASC,
        ),
        events=[
            replay_worker_pb2.ReplayEvent(
                event_id="evt-1",
                event_type="trade",
                instrument_id="BTC-USD",
                event_time=_ts("2026-03-27T09:05:00Z"),
                sequence=100,
                payload=payload,
            )
        ],
        simulated_time=_ts("2026-03-27T09:05:05Z"),
        end_of_stream=False,
        continuation_token="cont-2",
    )
    out = replay_worker_pb2.ReplayContext()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_replay_lookback_response_round_trip():
    payload = Struct()
    payload.update({"mid": 100.12})
    msg = replay_worker_pb2.ReplayLookbackResponse(
        replay=replay_worker_pb2.ReplayMetadata(
            contract=replay_worker_pb2.ContractVersion(schema_version=1),
            meta=replay_worker_pb2.CommonMetadata(
                correlation_id="corr-lb",
                causation_id="cause-lb",
                tenant_id="tenant-a",
                runtime_id="rt-123",
                launch_attempt=2,
            ),
            replay_session_id="replay-s-1",
            replay_cursor="cursor-99",
            order_key=replay_worker_pb2.EVENT_TIME_ASC,
        ),
        items=[
            replay_worker_pb2.HistoricalWindowItem(
                instrument_id="BTC-USD",
                event_time=_ts("2026-03-27T08:00:00Z"),
                sequence=1,
                payload=payload,
            )
        ],
        coverage=replay_worker_pb2.CoverageMetadata(
            coverage_status=replay_worker_pb2.COVERAGE_PARTIAL,
            truncated_by_policy=True,
            invalid_cursor=False,
            unsupported_request_shape=False,
            out_of_policy_range=False,
            reason_code="bounded_by_policy",
        ),
        next_continuation_token="next-token",
        served_at=_ts("2026-03-27T09:06:00Z"),
    )
    out = replay_worker_pb2.ReplayLookbackResponse()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_order_intent_round_trip():
    ext = risk_worker_pb2.CommonMetadata(
        causation_id="cause-oms-1",
        tenant_id="tenant-a",
        runtime_id="rt-123",
        worker_identity="worker-local-1",
        launch_attempt=2,
        strategy_version_id="sv-20260327",
    )
    msg = risk_worker_pb2.OrderIntent(
        correlation_id="corr-oms-1",
        account_id="acct-1",
        mode=risk_worker_pb2.PAPER,
        instrument_id="BTC-USD",
        side="BUY",
        order_type="LIMIT",
        quantity="1.25",
        limit_price="65000.10",
        stop_price="",
        time_in_force="GTC",
        idempotency_key="idem-1",
        requested_at=_ts("2026-03-27T09:07:00Z"),
        created_at=_ts("2026-03-27T09:06:30Z"),
        extension=ext,
    )
    out = risk_worker_pb2.OrderIntent()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_order_intent_ack_round_trip():
    msg = risk_worker_pb2.OrderIntentAck(
        meta=risk_worker_pb2.CommonMetadata(
            correlation_id="corr-oms-1",
            tenant_id="tenant-a",
            account_id="acct-1",
            runtime_id="rt-123",
            worker_identity="worker-local-1",
            launch_attempt=2,
        ),
        status=risk_worker_pb2.ACCEPTED,
        accepted=True,
        order_id="ord-5",
        state="order.acknowledged",
        reason_code="",
        error_code="",
        observed_at=_ts("2026-03-27T09:07:01Z"),
    )
    out = risk_worker_pb2.OrderIntentAck()
    out.ParseFromString(msg.SerializeToString())
    assert out == msg


def test_golden_launch_spec_compatibility_and_field_numbers():
    golden = {
        "contract": {"schemaVersion": 1, "compatibilityNote": "swr-102 freeze"},
        "correlationId": "corr-100",
        "causationId": "cause-99",
        "tenantId": "tenant-a",
        "accountId": "acct-1",
        "runtimeId": "rt-xyz",
        "workerIdentity": "worker-01",
        "launchAttempt": 1,
        "strategyVersionId": "sv-42",
        "jobId": "job-abc",
        "deploymentId": "dep-main",
        "mode": "PAPER",
        "traderId": "trader-1",
        "validatedParameterIdentity": "vp-1",
        "parameterHash": "sha256:abcd",
        "artifactReference": "artifact-ref",
        "artifactUri": "registry.local/ref",
        "artifactDigest": "sha256:ffff",
        "entrypoint": "pkg.main:run",
        "sandboxProfile": "restricted",
        "requestedBy": "runtime-manager",
        "requestedAt": "2026-03-27T09:00:00Z",
    }
    msg = json_format.ParseDict(golden, manager_worker_pb2.LaunchSpec())
    raw = msg.SerializeToString()
    golden_b64 = base64.b64encode(raw).decode("ascii")
    reparsed = manager_worker_pb2.LaunchSpec()
    reparsed.ParseFromString(base64.b64decode(golden_b64))
    assert reparsed == msg

    raw_with_unknown = _append_unknown_string_field(
        raw, field_number=99, value="future-additive"
    )
    forward_safe = manager_worker_pb2.LaunchSpec()
    forward_safe.ParseFromString(raw_with_unknown)
    assert _to_dict(forward_safe) == _to_dict(msg)

    assert _field_numbers(manager_worker_pb2.LaunchSpec) == {
        "contract": 1,
        "correlation_id": 2,
        "causation_id": 3,
        "tenant_id": 4,
        "account_id": 5,
        "runtime_id": 6,
        "worker_identity": 7,
        "launch_attempt": 8,
        "strategy_version_id": 9,
        "job_id": 10,
        "deployment_id": 11,
        "mode": 12,
        "trader_id": 13,
        "validated_parameter_identity": 14,
        "parameter_hash": 15,
        "artifact_reference": 16,
        "artifact_uri": 17,
        "artifact_digest": 18,
        "entrypoint": 19,
        "sandbox_profile": 20,
        "requested_by": 21,
        "requested_at": 22,
    }


def test_golden_heartbeat_compatibility_and_field_numbers():
    golden = {
        "contract": {"schemaVersion": 1},
        "eventId": "evt-hb",
        "eventName": "runtime.heartbeat",
        "eventVersion": 1,
        "producer": "strategy-worker-runtime",
        "correlationId": "corr-hb",
        "tenantId": "tenant-a",
        "accountId": "acct-1",
        "runtimeId": "rt-xyz",
        "workerIdentity": "worker-01",
        "launchAttempt": 1,
        "strategyVersionId": "sv-42",
        "occurredAt": "2026-03-27T09:10:00Z",
        "payload": {"localState": "RUNNING"},
    }
    msg = json_format.ParseDict(golden, manager_worker_pb2.Heartbeat())
    raw = msg.SerializeToString()
    reparsed = manager_worker_pb2.Heartbeat()
    reparsed.ParseFromString(raw)
    assert reparsed == msg

    raw_with_unknown = _append_unknown_string_field(
        raw, field_number=88, value="additive"
    )
    forward_safe = manager_worker_pb2.Heartbeat()
    forward_safe.ParseFromString(raw_with_unknown)
    assert _to_dict(forward_safe) == _to_dict(msg)

    assert _field_numbers(manager_worker_pb2.Heartbeat) == {
        "contract": 1,
        "event_id": 2,
        "event_name": 3,
        "event_version": 4,
        "producer": 5,
        "correlation_id": 6,
        "causation_id": 7,
        "tenant_id": 8,
        "account_id": 9,
        "runtime_id": 10,
        "worker_identity": 11,
        "launch_attempt": 12,
        "strategy_version_id": 13,
        "occurred_at": 14,
        "payload": 15,
    }


def test_golden_replay_context_compatibility_and_field_numbers():
    golden = {
        "replay": {
            "contract": {"schemaVersion": 1},
            "meta": {
                "correlationId": "corr-rp",
                "tenantId": "tenant-a",
                "runtimeId": "rt-xyz",
                "launchAttempt": 1,
            },
            "replaySessionId": "replay-1",
            "replayCursor": "cursor-1",
            "datasetVersion": "dataset-v1",
            "policySnapshotId": "policy-v2",
            "orderKey": "EVENT_TIME_ASC_SEQUENCE_ASC",
        },
        "events": [
            {
                "eventId": "evt-1",
                "eventType": "tick",
                "instrumentId": "BTC-USD",
                "eventTime": "2026-03-27T09:10:00Z",
                "sequence": "7",
                "payload": {"price": 123.4},
            }
        ],
        "simulatedTime": "2026-03-27T09:10:05Z",
        "endOfStream": False,
        "continuationToken": "cont-1",
    }
    msg = json_format.ParseDict(golden, replay_worker_pb2.ReplayContext())
    raw = msg.SerializeToString()
    reparsed = replay_worker_pb2.ReplayContext()
    reparsed.ParseFromString(raw)
    assert reparsed == msg

    raw_with_unknown = _append_unknown_string_field(
        raw, field_number=77, value="future"
    )
    forward_safe = replay_worker_pb2.ReplayContext()
    forward_safe.ParseFromString(raw_with_unknown)
    assert _to_dict(forward_safe) == _to_dict(msg)

    assert _field_numbers(replay_worker_pb2.ReplayContext) == {
        "replay": 1,
        "events": 2,
        "simulated_time": 3,
        "end_of_stream": 4,
        "continuation_token": 5,
    }


def test_golden_order_intent_compatibility_and_field_numbers():
    golden = {
        "correlationId": "corr-oms",
        "accountId": "acct-1",
        "mode": "PAPER",
        "instrumentId": "BTC-USD",
        "side": "BUY",
        "orderType": "LIMIT",
        "quantity": "1",
        "limitPrice": "65000",
        "stopPrice": "",
        "timeInForce": "GTC",
        "idempotencyKey": "idem-123",
        "requestedAt": "2026-03-27T09:10:00Z",
        "orderIntentId": 7,
        "symbol": "AAPL",
    }
    msg = json_format.ParseDict(golden, risk_worker_pb2.OrderIntent())
    raw = msg.SerializeToString()
    reparsed = risk_worker_pb2.OrderIntent()
    reparsed.ParseFromString(raw)
    assert reparsed == msg

    raw_with_unknown = _append_unknown_string_field(
        raw, field_number=66, value="future"
    )
    forward_safe = risk_worker_pb2.OrderIntent()
    forward_safe.ParseFromString(raw_with_unknown)
    assert _to_dict(forward_safe) == _to_dict(msg)

    assert _field_numbers(risk_worker_pb2.OrderIntent) == {
        "correlation_id": 2,
        "account_id": 3,
        "mode": 4,
        "instrument_id": 5,
        "side": 6,
        "order_type": 7,
        "quantity": 8,
        "limit_price": 9,
        "stop_price": 10,
        "time_in_force": 11,
        "idempotency_key": 12,
        "requested_at": 13,
        "extension": 15,
        "job_id": 16,
        "order_intent_id": 17,  # field number unchanged; wire type is string
        "symbol": 18,
        "created_at": 19,
    }


def test_boundary_worker_signals_are_execution_plane_only():
    # Worker signal schema includes launch/heartbeat/degraded/failure/termination
    # and intentionally excludes manager-owned runtime.started truth payloads.
    assert not hasattr(manager_worker_pb2, "RuntimeStarted")
    signal_methods = {
        "ReportLaunchSucceeded",
        "ReportLaunchFailed",
        "ReportHeartbeat",
        "ReportDegraded",
        "ReportTermination",
    }
    defined = {
        m.name
        for m in manager_worker_pb2.DESCRIPTOR.services_by_name[
            "WorkerLifecycleSignalService"
        ].methods
    }
    assert signal_methods == defined


def test_boundary_backtest_does_not_use_worker_intent_contract():
    replay_services = set(replay_worker_pb2.DESCRIPTOR.services_by_name.keys())
    worker_intent_services = set(risk_worker_pb2.DESCRIPTOR.services_by_name.keys())
    assert "OrderIntentService" not in replay_services
    assert "BacktestOrderIntentService" not in worker_intent_services


def test_boundary_replay_lookback_is_coverage_aware():
    fields = set(_field_numbers(replay_worker_pb2.CoverageMetadata).keys())
    assert {
        "coverage_status",
        "truncated_by_policy",
        "invalid_cursor",
        "unsupported_request_shape",
        "out_of_policy_range",
        "reason_code",
    }.issubset(fields)


def test_boundary_risk_worker_order_intent_has_scope_and_idempotency():
    fields = set(_field_numbers(risk_worker_pb2.OrderIntent).keys())
    assert {
        "mode",
        "idempotency_key",
        "correlation_id",
        "account_id",
        "job_id",
        "order_intent_id",
        "symbol",
        "created_at",
    }.issubset(fields)
    meta_fields = set(_field_numbers(risk_worker_pb2.CommonMetadata).keys())
    assert {
        "account_id",
        "runtime_id",
        "correlation_id",
        "causation_id",
        "job_id",
    }.issubset(meta_fields)
    assert "backtest_job_id" not in meta_fields
    replay_meta = set(_field_numbers(replay_worker_pb2.CommonMetadata).keys())
    assert "job_id" in replay_meta
    assert "backtest_job_id" not in replay_meta


def test_boundary_lifecycle_ordering_domain_present():
    fields = set(_field_numbers(manager_worker_pb2.LaunchSpec).keys())
    assert {"runtime_id", "launch_attempt", "job_id"}.issubset(fields)
