from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, cast

import grpc
from runtime.transport.grpc.serializers import (
    _ensure_generated_proto_path,
)


def _utc_like_for_historical_api(text: str) -> str:
    """Normalize launch ISO timestamps toward historical-data-service ``time_utc`` text style."""
    raw = text.strip()
    if not raw:
        return raw
    norm = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(norm)
    except ValueError:
        return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


class HistoricalDataGrpcClient:
    """Read-only client for ``HistoricalDataService/QueryHistoricalData``."""

    def __init__(
        self, channel: grpc.Channel, stub: object, *, timeout_seconds: float
    ) -> None:
        self._channel = channel
        self._stub = stub
        self._timeout_seconds = timeout_seconds

    def close(self) -> None:
        self._channel.close()

    def health_check(self, *, timeout_seconds: float | None = None) -> str:
        """Call ``HistoricalDataService/HealthCheck`` (DB ping on the server). Returns ``status`` text."""
        _ensure_generated_proto_path()
        import importlib

        historical_data_pb2 = importlib.import_module("historical_data_pb2")
        t = self._timeout_seconds if timeout_seconds is None else float(timeout_seconds)
        req = historical_data_pb2.HealthCheckRequest()
        stub = cast(Any, self._stub)
        resp = stub.HealthCheck(req, timeout=max(0.1, t))
        return str(getattr(resp, "status", "") or "")

    def query_historical_data(self, request: object) -> Any:
        return cast(Any, self._stub).QueryHistoricalData(
            request, timeout=self._timeout_seconds
        )


def _historical_time_utc_to_epoch_ms(time_utc: str) -> int | None:
    from datetime import datetime, timezone

    from runtime.integration.epoch_time import (
        utc_datetime_to_epoch_millis,
    )

    raw = time_utc.strip()
    if not raw:
        return None
    text = raw.replace("Z", "+00:00")
    if text.endswith("+00") and not text.endswith("+00:00"):
        text = text[:-3] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return utc_datetime_to_epoch_millis(dt.astimezone(timezone.utc))


def build_historical_bar_replay_tick(
    *,
    bar: object,
    symbol: str,
    timeframe: str,
    sequence: int,
) -> dict[str, Any]:
    """Build a replay tick dict that :class:`EventMapper` maps to ``market.bar``."""
    time_utc = str(getattr(bar, "time_utc", "") or "")
    ts_ms = _historical_time_utc_to_epoch_ms(time_utc)
    if ts_ms is None:
        raise ValueError(f"historical bar missing parseable time_utc: {time_utc!r}")

    iid = int(getattr(bar, "instrument_id", 0) or 0)
    sym = str(symbol).strip()
    return {
        "type": "market.bar",
        "event_type": "market.bar",
        "event_id": f"hds:{iid}:{time_utc}:{sequence}",
        "symbol": symbol,
        "instrument_id": sym if sym else str(iid or ""),
        "timeframe": timeframe,
        "open": float(getattr(bar, "open", 0.0) or 0.0),
        "high": float(getattr(bar, "high", 0.0) or 0.0),
        "low": float(getattr(bar, "low", 0.0) or 0.0),
        "close": float(getattr(bar, "close", 0.0) or 0.0),
        "volume": float(getattr(bar, "volume", 0) or 0),
        "ts_ms": int(ts_ms),
        "event_time": time_utc,
    }


def build_historical_data_grpc_client(
    *,
    target: str,
    timeout_seconds: float = 30.0,
) -> HistoricalDataGrpcClient | None:
    t = (target or "").strip()
    if not t:
        return None
    _ensure_generated_proto_path()
    import importlib

    importlib.import_module("historical_data_pb2")
    historical_data_pb2_grpc = importlib.import_module("historical_data_pb2_grpc")

    channel = grpc.insecure_channel(t)
    stub = historical_data_pb2_grpc.HistoricalDataServiceStub(channel)
    return HistoricalDataGrpcClient(channel, stub, timeout_seconds=timeout_seconds)


def print_historical_data_service_connectivity_at_startup(
    historical_data_grpc_target: str,
    client: HistoricalDataGrpcClient | None,
    *,
    probe_timeout_seconds: float = 5.0,
) -> None:
    """Log to stdout whether ``HistoricalDataService`` is reachable (``HealthCheck`` RPC)."""
    target = (historical_data_grpc_target or "").strip()
    prefix = "[historical-data-service]"
    if not target:
        print(
            f"{prefix} not configured: historical_data_grpc_target is empty; "
            "no gRPC client (backtest will not pull bars from HistoricalDataService).",
            flush=True,
        )
        return
    if client is None:
        print(
            f"{prefix} not configured: gRPC client is missing for target {target!r}.",
            flush=True,
        )
        return
    effective = max(0.1, float(probe_timeout_seconds))
    try:
        status = client.health_check(timeout_seconds=effective)
        print(
            f"{prefix} connected to {target!r} (HealthCheck OK, status={status!r}).",
            flush=True,
        )
    except grpc.RpcError as exc:
        code = exc.code().name if hasattr(exc, "code") else "UNKNOWN"
        details = exc.details() if hasattr(exc, "details") else str(exc)
        print(
            f"{prefix} NOT connected to {target!r}: gRPC {code} details={details!r}.",
            flush=True,
        )
    except Exception as exc:
        print(
            f"{prefix} NOT connected to {target!r}: {type(exc).__name__}: {exc!r}.",
            flush=True,
        )


def build_query_historical_request(
    *,
    symbol: str,
    timeframe: str,
    from_utc: str,
    to_utc: str,
    limit: int,
    cursor: str | None,
    warmup_bars: int,
    historical_data_pb2: Any,
) -> Any:
    pb = cast(Any, historical_data_pb2)
    req = pb.QueryHistoricalDataRequest(
        symbol=symbol,
        timeframe=timeframe,
        from_utc=_utc_like_for_historical_api(from_utc),
        to_utc=_utc_like_for_historical_api(to_utc),
        data_type=pb.HISTORICAL_DATA_TYPE_BAR,
        sort="asc",
        limit=max(1, int(limit)),
    )
    if cursor:
        req.cursor = cursor
    if warmup_bars > 0 and not cursor:
        req.warmup_bars = int(warmup_bars)
    return req


def launch_payload_backtest_symbol(payload: Mapping[str, object]) -> str | None:
    params = payload.get("parameters")
    if not isinstance(params, dict):
        return None
    sym = params.get("symbol")
    if isinstance(sym, str) and sym.strip():
        return sym.strip()
    return None
