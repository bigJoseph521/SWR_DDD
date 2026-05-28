"""Typed stdio JSON Lines messages between backtest-runner and runtime_backtest.

Wire shapes match ``backtest-runner`` ``protocol.rs`` and ``dto.rs``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, TypeAlias

# Runner → worker (stdin)
MESSAGE_TYPE_INITIALIZE_STRATEGY = "INITIALIZE_STRATEGY"
MESSAGE_TYPE_MARKET_DATA_EVENT = "MARKET_DATA_EVENT"
MESSAGE_TYPE_PORTFOLIO_SNAPSHOT = "PORTFOLIO_SNAPSHOT"
MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT = "OPEN_ORDERS_SNAPSHOT"
MESSAGE_TYPE_HEALTH_CHECK = "HEALTH_CHECK"
MESSAGE_TYPE_SHUTDOWN = "SHUTDOWN"

# Worker → runner (stdout)
MESSAGE_TYPE_STRATEGY_INITIALIZED = "STRATEGY_INITIALIZED"
MESSAGE_TYPE_ORDER_INTENTS = "ORDER_INTENTS"
MESSAGE_TYPE_NO_OP = "NO_OP"
MESSAGE_TYPE_STRATEGY_ERROR = "STRATEGY_ERROR"
MESSAGE_TYPE_WORKER_HEALTH = "WORKER_HEALTH"
MESSAGE_TYPE_SHUTDOWN_ACK = "SHUTDOWN_ACK"

RUNNER_REQUEST_TYPES: frozenset[str] = frozenset(
    {
        MESSAGE_TYPE_INITIALIZE_STRATEGY,
        MESSAGE_TYPE_MARKET_DATA_EVENT,
        MESSAGE_TYPE_PORTFOLIO_SNAPSHOT,
        MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT,
        MESSAGE_TYPE_HEALTH_CHECK,
        MESSAGE_TYPE_SHUTDOWN,
    }
)

RUNNER_RESPONSE_TYPES: frozenset[str] = frozenset(
    {
        MESSAGE_TYPE_STRATEGY_INITIALIZED,
        MESSAGE_TYPE_ORDER_INTENTS,
        MESSAGE_TYPE_NO_OP,
        MESSAGE_TYPE_STRATEGY_ERROR,
        MESSAGE_TYPE_WORKER_HEALTH,
        MESSAGE_TYPE_SHUTDOWN_ACK,
    }
)

LEGACY_MESSAGE_TYPES: frozenset[str] = frozenset({"INIT"})

ALL_MESSAGE_TYPES: frozenset[str] = (
    RUNNER_REQUEST_TYPES | RUNNER_RESPONSE_TYPES | LEGACY_MESSAGE_TYPES
)

# Legacy aliases (tests / older docs)
INBOUND_MESSAGE_TYPES = RUNNER_REQUEST_TYPES
OUTBOUND_MESSAGE_TYPES = RUNNER_RESPONSE_TYPES


def _require_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _optional_str(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string when set")
    stripped = value.strip()
    return stripped or None


def _require_mapping(data: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{key} must be a JSON object")
    return dict(value)


def _require_list(data: Mapping[str, Any], key: str) -> list[Any]:
    value = data.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a JSON array")
    return list(value)


def _coerce_sequence(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("sequence must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise ValueError("sequence must be an integer")


@dataclass(frozen=True, slots=True)
class WireEnvelope:
    """Fields present on every backtest-runner protocol frame."""

    sequence: int
    backtest_job_id: str
    runtime_id: str
    message_id: str | None = None
    correlation_id: str | None = None
    timestamp: str | None = None
    backtest_time: str | None = None

    def envelope_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "sequence": self.sequence,
            "backtest_job_id": self.backtest_job_id,
            "runtime_id": self.runtime_id,
        }
        if self.message_id is not None:
            out["message_id"] = self.message_id
        if self.correlation_id is not None:
            out["correlation_id"] = self.correlation_id
        if self.timestamp is not None:
            out["timestamp"] = self.timestamp
        if self.backtest_time is not None:
            out["backtest_time"] = self.backtest_time
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WireEnvelope:
        return cls(
            sequence=_coerce_sequence(data.get("sequence")),
            backtest_job_id=_require_str(data, "backtest_job_id"),
            runtime_id=_require_str(data, "runtime_id"),
            message_id=_optional_str(data, "message_id"),
            correlation_id=_optional_str(data, "correlation_id"),
            timestamp=_optional_str(data, "timestamp"),
            backtest_time=_optional_str(data, "backtest_time"),
        )


@dataclass(frozen=True, slots=True)
class InitializeStrategyPayload:
    strategy_id: str | None = None
    parameters: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.strategy_id is not None:
            out["strategy_id"] = self.strategy_id
        if self.parameters is not None:
            out["parameters"] = dict(self.parameters)
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InitializeStrategyPayload:
        strategy_id = _optional_str(data, "strategy_id")
        params = data.get("parameters")
        parameters: dict[str, Any] | None = None
        if params is not None:
            if not isinstance(params, Mapping):
                raise ValueError("parameters must be a JSON object when set")
            parameters = dict(params)
        return cls(strategy_id=strategy_id, parameters=parameters)


@dataclass(frozen=True, slots=True)
class BarWireDto:
    open: str
    high: str
    low: str
    close: str
    volume: str

    def to_dict(self) -> dict[str, str]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BarWireDto:
        return cls(
            open=_require_str(data, "open"),
            high=_require_str(data, "high"),
            low=_require_str(data, "low"),
            close=_require_str(data, "close"),
            volume=_require_str(data, "volume"),
        )


@dataclass(frozen=True, slots=True)
class MarketDataEventPayload:
    instrument_id: str
    symbol: str
    bar: BarWireDto

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "bar": self.bar.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarketDataEventPayload:
        bar = data.get("bar")
        if not isinstance(bar, Mapping):
            raise ValueError("bar must be a JSON object")
        return cls(
            instrument_id=_require_str(data, "instrument_id"),
            symbol=_require_str(data, "symbol"),
            bar=BarWireDto.from_dict(bar),
        )


@dataclass(frozen=True, slots=True)
class PortfolioSnapshotPayload:
    positions: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"positions": list(self.positions)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PortfolioSnapshotPayload:
        return cls(positions=_require_list(data, "positions"))


@dataclass(frozen=True, slots=True)
class OpenOrdersSnapshotPayload:
    orders: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"orders": list(self.orders)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OpenOrdersSnapshotPayload:
        return cls(orders=_require_list(data, "orders"))


@dataclass(frozen=True, slots=True)
class HealthCheckPayload:
    nonce: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.nonce is not None:
            out["nonce"] = self.nonce
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HealthCheckPayload:
        return cls(nonce=_optional_str(data, "nonce"))


@dataclass(frozen=True, slots=True)
class ShutdownRequestPayload:
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.reason is not None:
            out["reason"] = self.reason
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ShutdownRequestPayload:
        return cls(reason=_optional_str(data, "reason"))


@dataclass(frozen=True, slots=True)
class StrategyInitializedPayload:
    strategy_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.strategy_id is not None:
            out["strategy_id"] = self.strategy_id
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StrategyInitializedPayload:
        return cls(strategy_id=_optional_str(data, "strategy_id"))


@dataclass(frozen=True, slots=True)
class StrategyErrorPayload:
    code: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StrategyErrorPayload:
        return cls(
            code=_require_str(data, "code"), message=_require_str(data, "message")
        )


@dataclass(frozen=True, slots=True)
class WorkerHealthPayload:
    status: str
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"status": self.status}
        if self.detail is not None:
            out["detail"] = self.detail
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkerHealthPayload:
        return cls(
            status=_require_str(data, "status"),
            detail=_optional_str(data, "detail"),
        )


@dataclass(frozen=True, slots=True)
class ShutdownAckPayload:
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.reason is not None:
            out["reason"] = self.reason
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ShutdownAckPayload:
        return cls(reason=_optional_str(data, "reason"))


@dataclass(frozen=True, slots=True)
class NoOpPayload:
    def to_dict(self) -> dict[str, Any]:
        return {}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NoOpPayload:
        _ = data
        return cls()


def _wire_request_dict(
    message_type: str,
    envelope: WireEnvelope,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    out = {"type": message_type, **envelope.envelope_dict()}
    if payload is not None:
        out["payload"] = dict(payload)
    return out


@dataclass(frozen=True, slots=True)
class InitializeStrategyRequest:
    envelope: WireEnvelope
    payload: InitializeStrategyPayload
    type: Literal["INITIALIZE_STRATEGY"] = MESSAGE_TYPE_INITIALIZE_STRATEGY

    def to_dict(self) -> dict[str, Any]:
        return _wire_request_dict(self.type, self.envelope, self.payload.to_dict())


@dataclass(frozen=True, slots=True)
class MarketDataEventRequest:
    envelope: WireEnvelope
    payload: MarketDataEventPayload
    type: Literal["MARKET_DATA_EVENT"] = MESSAGE_TYPE_MARKET_DATA_EVENT

    def to_dict(self) -> dict[str, Any]:
        return _wire_request_dict(self.type, self.envelope, self.payload.to_dict())


@dataclass(frozen=True, slots=True)
class PortfolioSnapshotRequest:
    envelope: WireEnvelope
    payload: PortfolioSnapshotPayload
    type: Literal["PORTFOLIO_SNAPSHOT"] = MESSAGE_TYPE_PORTFOLIO_SNAPSHOT

    def to_dict(self) -> dict[str, Any]:
        return _wire_request_dict(self.type, self.envelope, self.payload.to_dict())


@dataclass(frozen=True, slots=True)
class OpenOrdersSnapshotRequest:
    envelope: WireEnvelope
    payload: OpenOrdersSnapshotPayload
    type: Literal["OPEN_ORDERS_SNAPSHOT"] = MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT

    def to_dict(self) -> dict[str, Any]:
        return _wire_request_dict(self.type, self.envelope, self.payload.to_dict())


@dataclass(frozen=True, slots=True)
class HealthCheckRequest:
    envelope: WireEnvelope
    payload: HealthCheckPayload | None = None
    type: Literal["HEALTH_CHECK"] = MESSAGE_TYPE_HEALTH_CHECK

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload.to_dict() if self.payload is not None else {}
        return _wire_request_dict(self.type, self.envelope, payload)


@dataclass(frozen=True, slots=True)
class ShutdownRequest:
    envelope: WireEnvelope
    payload: ShutdownRequestPayload | None = None
    type: Literal["SHUTDOWN"] = MESSAGE_TYPE_SHUTDOWN

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload.to_dict() if self.payload is not None else {}
        return _wire_request_dict(self.type, self.envelope, payload)


def _wire_response_dict(
    message_type: str,
    envelope: WireEnvelope,
    *,
    payload: Mapping[str, Any] | None = None,
    intents: list[Any] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {"type": message_type, **envelope.envelope_dict()}
    if payload is not None:
        out["payload"] = dict(payload)
    if intents is not None:
        out["intents"] = list(intents)
    return out


@dataclass(frozen=True, slots=True)
class StrategyInitializedResponse:
    envelope: WireEnvelope
    payload: StrategyInitializedPayload
    type: Literal["STRATEGY_INITIALIZED"] = MESSAGE_TYPE_STRATEGY_INITIALIZED

    def to_dict(self) -> dict[str, Any]:
        return _wire_response_dict(
            self.type, self.envelope, payload=self.payload.to_dict()
        )


@dataclass(frozen=True, slots=True)
class OrderIntentsResponse:
    envelope: WireEnvelope
    intents: list[Any] = field(default_factory=list)
    type: Literal["ORDER_INTENTS"] = MESSAGE_TYPE_ORDER_INTENTS

    def to_dict(self) -> dict[str, Any]:
        return _wire_response_dict(self.type, self.envelope, intents=self.intents)


@dataclass(frozen=True, slots=True)
class NoOpResponse:
    envelope: WireEnvelope
    payload: NoOpPayload | None = None
    type: Literal["NO_OP"] = MESSAGE_TYPE_NO_OP

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload.to_dict() if self.payload is not None else {}
        return _wire_response_dict(self.type, self.envelope, payload=payload)


@dataclass(frozen=True, slots=True)
class StrategyErrorResponse:
    envelope: WireEnvelope
    payload: StrategyErrorPayload
    type: Literal["STRATEGY_ERROR"] = MESSAGE_TYPE_STRATEGY_ERROR

    def to_dict(self) -> dict[str, Any]:
        return _wire_response_dict(
            self.type, self.envelope, payload=self.payload.to_dict()
        )


@dataclass(frozen=True, slots=True)
class WorkerHealthResponse:
    envelope: WireEnvelope
    payload: WorkerHealthPayload
    type: Literal["WORKER_HEALTH"] = MESSAGE_TYPE_WORKER_HEALTH

    def to_dict(self) -> dict[str, Any]:
        return _wire_response_dict(
            self.type, self.envelope, payload=self.payload.to_dict()
        )


@dataclass(frozen=True, slots=True)
class ShutdownAckResponse:
    envelope: WireEnvelope
    payload: ShutdownAckPayload | None = None
    type: Literal["SHUTDOWN_ACK"] = MESSAGE_TYPE_SHUTDOWN_ACK

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload.to_dict() if self.payload is not None else {}
        return _wire_response_dict(self.type, self.envelope, payload=payload)


# Session-layer replay messages (converted from runner wire by host/runner_wire.py)


@dataclass(frozen=True, slots=True)
class PortfolioSnapshotMessage:
    portfolio: dict[str, Any] = field(default_factory=dict)
    type: Literal["PORTFOLIO_SNAPSHOT"] = MESSAGE_TYPE_PORTFOLIO_SNAPSHOT

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "portfolio": dict(self.portfolio)}


@dataclass(frozen=True, slots=True)
class OpenOrdersSnapshotMessage:
    orders: list[Any] = field(default_factory=list)
    type: Literal["OPEN_ORDERS_SNAPSHOT"] = MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "orders": list(self.orders)}


@dataclass(frozen=True, slots=True)
class MarketDataEventMessage:
    event: dict[str, Any]
    type: Literal["MARKET_DATA_EVENT"] = MESSAGE_TYPE_MARKET_DATA_EVENT

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "event": dict(self.event)}


@dataclass(frozen=True, slots=True)
class OrderIntentsMessage:
    intents: list[Any] = field(default_factory=list)
    type: Literal["ORDER_INTENTS"] = MESSAGE_TYPE_ORDER_INTENTS

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "intents": list(self.intents)}


@dataclass(frozen=True, slots=True)
class NoOpMessage:
    type: Literal["NO_OP"] = MESSAGE_TYPE_NO_OP

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type}


@dataclass(frozen=True, slots=True)
class StrategyErrorMessage:
    code: str
    message: str
    type: Literal["STRATEGY_ERROR"] = MESSAGE_TYPE_STRATEGY_ERROR

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class InitMessage:
    """Legacy stdio ``INIT`` shape (unit tests and deprecated native host only)."""

    protocol_version: str
    backtest_job_id: str
    artifact_uri: str
    entrypoint: str
    strategy_id: str = ""
    strategy_version_id: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    time_range: dict[str, Any] = field(default_factory=dict)
    symbols: list[Any] = field(default_factory=list)
    bar_resolution: str = ""
    type: Literal["INIT"] = "INIT"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "protocol_version": self.protocol_version,
            "backtest_job_id": self.backtest_job_id,
            "strategy_id": self.strategy_id,
            "strategy_version_id": self.strategy_version_id,
            "artifact_uri": self.artifact_uri,
            "entrypoint": self.entrypoint,
            "parameters": dict(self.parameters),
            "time_range": dict(self.time_range),
            "symbols": list(self.symbols),
            "bar_resolution": self.bar_resolution,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InitMessage:
        return cls(
            protocol_version=_require_str(data, "protocol_version"),
            backtest_job_id=_require_str(data, "backtest_job_id"),
            artifact_uri=_require_str(data, "artifact_uri"),
            entrypoint=_require_str(data, "entrypoint"),
            strategy_id=_optional_str(data, "strategy_id") or "",
            strategy_version_id=_optional_str(data, "strategy_version_id") or "",
            parameters=_require_mapping(data, "parameters"),
            time_range=_require_mapping(data, "time_range"),
            symbols=_require_list(data, "symbols"),
            bar_resolution=_optional_str(data, "bar_resolution") or "",
        )


# Backward-compatible aliases
ErrorMessage = StrategyErrorMessage
ReadyMessage = StrategyInitializedResponse
AckMessage = NoOpMessage
CompletedMessage = ShutdownAckResponse
EndOfStreamMessage = ShutdownRequest

InboundMessage: TypeAlias = (
    InitMessage
    | PortfolioSnapshotMessage
    | OpenOrdersSnapshotMessage
    | MarketDataEventMessage
)

OutboundMessage: TypeAlias = OrderIntentsMessage | NoOpMessage | StrategyErrorMessage

RunnerRequest: TypeAlias = (
    InitializeStrategyRequest
    | MarketDataEventRequest
    | PortfolioSnapshotRequest
    | OpenOrdersSnapshotRequest
    | HealthCheckRequest
    | ShutdownRequest
)

RunnerResponse: TypeAlias = (
    StrategyInitializedResponse
    | OrderIntentsResponse
    | NoOpResponse
    | StrategyErrorResponse
    | WorkerHealthResponse
    | ShutdownAckResponse
)

JsonlMessage: TypeAlias = (
    RunnerRequest | RunnerResponse | InboundMessage | OutboundMessage
)

_REQUEST_PARSERS: dict[str, type[RunnerRequest]] = {
    MESSAGE_TYPE_INITIALIZE_STRATEGY: InitializeStrategyRequest,
    MESSAGE_TYPE_MARKET_DATA_EVENT: MarketDataEventRequest,
    MESSAGE_TYPE_PORTFOLIO_SNAPSHOT: PortfolioSnapshotRequest,
    MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT: OpenOrdersSnapshotRequest,
    MESSAGE_TYPE_HEALTH_CHECK: HealthCheckRequest,
    MESSAGE_TYPE_SHUTDOWN: ShutdownRequest,
}

_RESPONSE_PARSERS: dict[str, type[RunnerResponse]] = {
    MESSAGE_TYPE_STRATEGY_INITIALIZED: StrategyInitializedResponse,
    MESSAGE_TYPE_ORDER_INTENTS: OrderIntentsResponse,
    MESSAGE_TYPE_NO_OP: NoOpResponse,
    MESSAGE_TYPE_STRATEGY_ERROR: StrategyErrorResponse,
    MESSAGE_TYPE_WORKER_HEALTH: WorkerHealthResponse,
    MESSAGE_TYPE_SHUTDOWN_ACK: ShutdownAckResponse,
}


def _parse_wire_request(message_type: str, data: Mapping[str, Any]) -> RunnerRequest:
    envelope = WireEnvelope.from_dict(data)
    payload_raw = data.get("payload")
    payload_dict = dict(payload_raw) if isinstance(payload_raw, Mapping) else {}

    if message_type == MESSAGE_TYPE_INITIALIZE_STRATEGY:
        return InitializeStrategyRequest(
            envelope=envelope,
            payload=InitializeStrategyPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_MARKET_DATA_EVENT:
        return MarketDataEventRequest(
            envelope=envelope,
            payload=MarketDataEventPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_PORTFOLIO_SNAPSHOT:
        return PortfolioSnapshotRequest(
            envelope=envelope,
            payload=PortfolioSnapshotPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT:
        return OpenOrdersSnapshotRequest(
            envelope=envelope,
            payload=OpenOrdersSnapshotPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_HEALTH_CHECK:
        return HealthCheckRequest(
            envelope=envelope,
            payload=HealthCheckPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_SHUTDOWN:
        return ShutdownRequest(
            envelope=envelope,
            payload=ShutdownRequestPayload.from_dict(payload_dict),
        )
    raise ValueError(f"unsupported request type: {message_type!r}")


def _parse_wire_response(message_type: str, data: Mapping[str, Any]) -> RunnerResponse:
    envelope = WireEnvelope.from_dict(data)
    if message_type == MESSAGE_TYPE_ORDER_INTENTS:
        return OrderIntentsResponse(
            envelope=envelope,
            intents=_require_list(data, "intents"),
        )
    payload_raw = data.get("payload")
    payload_dict = dict(payload_raw) if isinstance(payload_raw, Mapping) else {}

    if message_type == MESSAGE_TYPE_STRATEGY_INITIALIZED:
        return StrategyInitializedResponse(
            envelope=envelope,
            payload=StrategyInitializedPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_NO_OP:
        return NoOpResponse(
            envelope=envelope, payload=NoOpPayload.from_dict(payload_dict)
        )
    if message_type == MESSAGE_TYPE_STRATEGY_ERROR:
        return StrategyErrorResponse(
            envelope=envelope,
            payload=StrategyErrorPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_WORKER_HEALTH:
        return WorkerHealthResponse(
            envelope=envelope,
            payload=WorkerHealthPayload.from_dict(payload_dict),
        )
    if message_type == MESSAGE_TYPE_SHUTDOWN_ACK:
        return ShutdownAckResponse(
            envelope=envelope,
            payload=ShutdownAckPayload.from_dict(payload_dict),
        )
    raise ValueError(f"unsupported response type: {message_type!r}")


def _is_legacy_session_frame(message_type: str, data: Mapping[str, Any]) -> bool:
    if message_type == "INIT":
        return True
    if message_type == MESSAGE_TYPE_MARKET_DATA_EVENT and "event" in data:
        return True
    if message_type == MESSAGE_TYPE_PORTFOLIO_SNAPSHOT and "portfolio" in data:
        return True
    if message_type == MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT and "orders" in data:
        return "payload" not in data
    if message_type == MESSAGE_TYPE_ORDER_INTENTS and "intents" in data:
        return "sequence" not in data
    if message_type == MESSAGE_TYPE_NO_OP and "sequence" not in data:
        return True
    if message_type == MESSAGE_TYPE_STRATEGY_ERROR and "code" in data:
        return "payload" not in data
    return False


def parse_message(data: Mapping[str, Any]) -> JsonlMessage:
    """Parse a JSON object into a typed protocol message."""
    raw_type = data.get("type")
    if not isinstance(raw_type, str) or not raw_type.strip():
        raise ValueError("type must be a non-empty string")
    message_type = raw_type.strip()

    if _is_legacy_session_frame(message_type, data):
        if message_type == "INIT":
            return InitMessage.from_dict(data)
        if message_type == MESSAGE_TYPE_MARKET_DATA_EVENT:
            event = data.get("event")
            if not isinstance(event, Mapping):
                raise ValueError("event must be a JSON object")
            return MarketDataEventMessage(event=dict(event))
        if message_type == MESSAGE_TYPE_PORTFOLIO_SNAPSHOT:
            return PortfolioSnapshotMessage(
                portfolio=_require_mapping(data, "portfolio")
            )
        if message_type == MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT:
            return OpenOrdersSnapshotMessage(orders=_require_list(data, "orders"))
        if message_type == MESSAGE_TYPE_ORDER_INTENTS:
            return OrderIntentsMessage(intents=_require_list(data, "intents"))
        if message_type == MESSAGE_TYPE_NO_OP:
            return NoOpMessage()
        if message_type == MESSAGE_TYPE_STRATEGY_ERROR:
            return StrategyErrorMessage(
                code=_require_str(data, "code"),
                message=_require_str(data, "message"),
            )

    if message_type in _REQUEST_PARSERS:
        return _parse_wire_request(message_type, data)
    if message_type in _RESPONSE_PARSERS:
        return _parse_wire_response(message_type, data)

    raise ValueError(f"unsupported message type: {message_type!r}")
