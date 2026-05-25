from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _ensure_generated_proto_path() -> Path:
    generated_dir = Path(__file__).resolve().parents[3] / "protos" / "generated"
    generated_dir_str = str(generated_dir)
    if generated_dir_str not in sys.path:
        sys.path.insert(0, generated_dir_str)
    return generated_dir


_ensure_generated_proto_path()
risk_worker_pb2 = importlib.import_module("risk_worker_pb2")
risk_worker_pb2_grpc = importlib.import_module("risk_worker_pb2_grpc")


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
    "_ensure_generated_proto_path",
    "_proto_ts_from_datetime",
    "risk_worker_pb2",
    "risk_worker_pb2_grpc",
]
