from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

import grpc
from google.protobuf.struct_pb2 import Struct, Value
from google.protobuf.timestamp_pb2 import Timestamp
from runtime.transport.grpc.oms_client import DependencyClientError
from runtime.transport.grpc.serializers import (
    replay_worker_pb2,
    replay_worker_pb2_grpc,
)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _to_proto_timestamp(value: datetime | None) -> Any:
    ts = Timestamp()
    if value is None:
        value = _utc_now()
    ts.FromDatetime(value.astimezone(timezone.utc))
    return ts


def _normalize_grpc_error(exc: grpc.RpcError) -> DependencyClientError:
    status_code = exc.code()
    details = {"grpc_code": str(status_code), "grpc_details": exc.details()}

    if status_code == grpc.StatusCode.NOT_FOUND:
        return DependencyClientError(
            code="DEPENDENCY_NOT_FOUND",
            message="Replay dependency entity not found.",
            retryable=False,
            details=details,
        )
    if status_code == grpc.StatusCode.INVALID_ARGUMENT:
        return DependencyClientError(
            code="DEPENDENCY_VALIDATION_FAILED",
            message="Replay dependency rejected request validation.",
            retryable=False,
            details=details,
        )
    if status_code in {
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
    }:
        return DependencyClientError(
            code="DEPENDENCY_ACCESS_DENIED",
            message="Replay dependency denied request by policy/access.",
            retryable=False,
            details=details,
        )
    if status_code in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
        return DependencyClientError(
            code="DEPENDENCY_UNAVAILABLE",
            message="Replay dependency is unavailable.",
            retryable=True,
            details=details,
        )
    return DependencyClientError(
        code="DEPENDENCY_INTERNAL_FAILURE",
        message="Replay dependency call failed unexpectedly.",
        retryable=False,
        details=details,
    )


class ReplayGrpcClient:
    """
    Thin adapter for worker-approved replay/backtest transport only.
    """

    def __init__(
        self,
        ingress_stub: Any,
        lookback_stub: Any,
        backtest_order_stub: Any,
        *,
        timeout_seconds: float = 3.0,
    ) -> None:
        self._ingress_stub = ingress_stub
        self._lookback_stub = lookback_stub
        self._backtest_order_stub = backtest_order_stub
        self._timeout_seconds = timeout_seconds

    def ingest_replay_tick(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Optional hook used by :class:`ReplayGateway`; ticks may instead arrive via the
        worker's replay ingress server. Implemented so one client can satisfy both paths.
        """
        _ = payload
        return {"accepted": True}

    def push_replay_context(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raw_event_payload = payload.get("payload")
        event_payload: Mapping[str, Any] = (
            raw_event_payload if isinstance(raw_event_payload, Mapping) else {}
        )
        request = replay_worker_pb2.ReplayContext(
            replay=replay_worker_pb2.ReplayMetadata(
                meta=replay_worker_pb2.CommonMetadata(
                    correlation_id=str(payload.get("correlation_id") or ""),
                    causation_id=str(payload.get("causation_id") or ""),
                    runtime_id=str(payload.get("runtime_id") or ""),
                    launch_attempt=int(payload.get("launch_attempt") or 0),
                    strategy_version_id=str(payload.get("strategy_version_id") or ""),
                    job_id=str(payload.get("job_id") or ""),
                ),
                replay_session_id=str(payload.get("replay_session_id") or ""),
                replay_cursor=str(payload.get("replay_cursor") or ""),
                dataset_version=str(payload.get("dataset_version") or ""),
            ),
            events=[
                replay_worker_pb2.ReplayEvent(
                    event_id=str(payload.get("event_id") or ""),
                    event_type=str(payload.get("event_type") or ""),
                    instrument_id=str(payload.get("instrument_id") or ""),
                    event_time=_to_proto_timestamp(payload.get("event_time")),
                    sequence=int(payload.get("sequence") or 0),
                    payload=Struct(
                        fields={
                            key: Value(string_value=str(value))
                            for key, value in event_payload.items()
                        }
                    ),
                )
            ],
            simulated_time=_to_proto_timestamp(payload.get("simulated_time")),
            end_of_stream=bool(payload.get("end_of_stream", False)),
            continuation_token=str(payload.get("continuation_token") or ""),
        )
        try:
            ack = self._ingress_stub.PushReplayContext(
                request, timeout=self._timeout_seconds
            )
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc
        return {
            "runtime_id": ack.runtime_id or None,
            "replay_session_id": ack.replay_session_id or None,
            "replay_cursor": ack.replay_cursor or None,
            "consumed_count": int(ack.consumed_count),
        }

    def get_replay_lookback(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        request = replay_worker_pb2.ReplayLookbackRequest(
            replay=replay_worker_pb2.ReplayMetadata(
                meta=replay_worker_pb2.CommonMetadata(
                    correlation_id=str(payload.get("correlation_id") or ""),
                    runtime_id=str(payload.get("runtime_id") or ""),
                    launch_attempt=int(payload.get("launch_attempt") or 0),
                    job_id=str(payload.get("job_id") or ""),
                ),
                replay_session_id=str(payload.get("replay_session_id") or ""),
                replay_cursor=str(payload.get("replay_cursor") or ""),
            ),
            instrument_id=str(payload.get("instrument_id") or ""),
            dataset_family=str(payload.get("dataset_family") or ""),
            start_time=_to_proto_timestamp(payload.get("start_time")),
            end_time=_to_proto_timestamp(payload.get("end_time")),
            limit=int(payload.get("limit") or 0),
            request_purpose=str(payload.get("request_purpose") or ""),
            continuation_token=str(payload.get("continuation_token") or ""),
        )
        try:
            response = self._lookback_stub.GetReplayLookback(
                request, timeout=self._timeout_seconds
            )
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc
        cov_rc = str(response.coverage.reason_code or "").strip()
        return {
            "coverage_status": replay_worker_pb2.ReplayCoverageStatus.Name(
                response.coverage.coverage_status
            ),
            "reason_code": cov_rc.upper() if cov_rc else None,
            "item_count": len(response.items),
            "next_continuation_token": response.next_continuation_token or None,
        }

    def submit_backtest_order_intent(
        self, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        def _coerce_dt(raw: object) -> datetime | None:
            if not isinstance(raw, datetime):
                return None
            dt = raw
            if dt.tzinfo is None or dt.utcoffset() is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        raw_wall = payload.get("requested_at") or payload.get("occurred_at")
        wall_dt = _coerce_dt(raw_wall) or _utc_now()
        raw_created = payload.get("created_at")
        created_dt = _coerce_dt(raw_created) or wall_dt
        request = replay_worker_pb2.BacktestOrderIntent(
            meta=replay_worker_pb2.CommonMetadata(
                correlation_id=str(payload.get("correlation_id") or ""),
                account_id=str(payload.get("account_id") or ""),
                runtime_id=str(payload.get("runtime_id") or ""),
                launch_attempt=0,
                job_id=str(payload.get("job_id") or ""),
            ),
            replay_session_id="",
            instrument_id=str(payload.get("instrument_id") or ""),
            side=str(payload.get("side") or ""),
            order_type=str(payload.get("order_type") or ""),
            tif=str(payload.get("time_in_force") or ""),
            quantity=str(payload.get("quantity") or ""),
            price=str(payload.get("limit_price") or ""),
            stop_price=str(payload.get("stop_price") or ""),
            client_order_ref=str(payload.get("idempotency_key") or ""),
            requested_at=_to_proto_timestamp(wall_dt),
            created_at=_to_proto_timestamp(created_dt),
            order_intent_id=str(payload.get("order_intent_id") or "").strip(),
        )
        try:
            ack = self._backtest_order_stub.SubmitBacktestOrderIntent(
                request,
                timeout=self._timeout_seconds,
            )
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc
        brc = str(ack.reason_code or "").strip()
        return {
            "accepted": bool(ack.accepted),
            "runtime_id": ack.runtime_id or None,
            "replay_session_id": ack.replay_session_id or None,
            "reason_code": brc.upper() if brc else None,
        }


def build_replay_grpc_client(
    *, target: str, timeout_seconds: float = 3.0
) -> ReplayGrpcClient | None:
    """
    When ``target`` is non-empty (``host:port``), returns a client with backtest order
    submission and no-op :meth:`ingest_replay_tick` for :class:`ReplayGateway`.
    """
    t = (target or "").strip()
    if not t:
        return None
    channel = grpc.insecure_channel(t)
    ingress = replay_worker_pb2_grpc.ReplayIngressServiceStub(channel)
    lookback = replay_worker_pb2_grpc.ReplayLookbackServiceStub(channel)
    backtest = replay_worker_pb2_grpc.BacktestOrderIntentServiceStub(channel)
    return ReplayGrpcClient(
        ingress, lookback, backtest, timeout_seconds=timeout_seconds
    )
