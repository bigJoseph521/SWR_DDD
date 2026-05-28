from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from runtime.infrastructure.grpc import risk_worker_pb2, risk_worker_pb2_grpc


def _proto_ts_from_datetime(value: datetime | None) -> Any:
    from google.protobuf.timestamp_pb2 import Timestamp

    ts = Timestamp()
    if value is None:
        return ts
    ts.FromDatetime(value.astimezone(timezone.utc))
    return ts


def _datetime_from_proto_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if not value.ListFields():
        return None
    return value.ToDatetime().astimezone(timezone.utc)


__all__ = [
    "_datetime_from_proto_ts",
    "_proto_ts_from_datetime",
    "risk_worker_pb2",
    "risk_worker_pb2_grpc",
]
