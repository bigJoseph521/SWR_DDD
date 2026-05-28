"""backtest-runner stdin/stdout JSONL wire (envelope + payload shapes)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from runtime.application.backtest_runner.messages import (
    MarketDataEventMessage,
    OpenOrdersSnapshotMessage,
    OrderIntentsMessage,
    OutboundMessage,
    PortfolioSnapshotMessage,
    StrategyErrorMessage,
)
from runtime.application.backtest_runner.session import BacktestRunnerSession
from runtime.interface.stdio.backtest_runner.wire.messages import (
    MESSAGE_TYPE_HEALTH_CHECK,
    MESSAGE_TYPE_INITIALIZE_STRATEGY,
    MESSAGE_TYPE_MARKET_DATA_EVENT,
    MESSAGE_TYPE_NO_OP,
    MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT,
    MESSAGE_TYPE_ORDER_INTENTS,
    MESSAGE_TYPE_PORTFOLIO_SNAPSHOT,
    MESSAGE_TYPE_SHUTDOWN,
    MESSAGE_TYPE_SHUTDOWN_ACK,
    MESSAGE_TYPE_STRATEGY_ERROR,
    MESSAGE_TYPE_STRATEGY_INITIALIZED,
    MESSAGE_TYPE_WORKER_HEALTH,
    RUNNER_REQUEST_TYPES,
    WireEnvelope,
)

_JSON_SEPARATORS = (",", ":")


class RunnerWireDecodeError(Exception):
    """Invalid runner JSONL frame."""


@dataclass(frozen=True, slots=True)
class RunnerWireFrame:
    envelope: WireEnvelope
    message_type: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RunnerWireControl:
    kind: str
    envelope: WireEnvelope
    payload: dict[str, Any]


def encode_jsonl_line(payload: Mapping[str, Any]) -> str:
    line = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=_JSON_SEPARATORS,
    )
    return f"{line}\n"


def decode_runner_line(
    line: str,
    *,
    line_number: int | None = None,
) -> RunnerWireFrame | RunnerWireControl:
    trimmed = line.strip()
    if not trimmed:
        hint = f" (line {line_number})" if line_number is not None else ""
        raise RunnerWireDecodeError(f"JSONL line must not be empty{hint}")

    try:
        raw = json.loads(trimmed)
    except json.JSONDecodeError as exc:
        raise RunnerWireDecodeError(f"invalid JSON: {exc.msg}") from exc

    if not isinstance(raw, dict):
        raise RunnerWireDecodeError("protocol frame must be a JSON object")

    message_type = raw.get("type")
    if not isinstance(message_type, str) or not message_type.strip():
        raise RunnerWireDecodeError("type must be a non-empty string")
    normalized = message_type.strip()
    if normalized not in RUNNER_REQUEST_TYPES:
        raise RunnerWireDecodeError(f"unsupported runner message type: {normalized!r}")

    try:
        envelope = WireEnvelope.from_dict(raw)
    except ValueError as exc:
        raise RunnerWireDecodeError(str(exc)) from exc

    payload = raw.get("payload")
    if payload is None:
        payload_dict: dict[str, Any] = {}
    elif isinstance(payload, Mapping):
        payload_dict = dict(payload)
    else:
        raise RunnerWireDecodeError("payload must be a JSON object when set")

    if normalized in (
        MESSAGE_TYPE_SHUTDOWN,
        MESSAGE_TYPE_HEALTH_CHECK,
        MESSAGE_TYPE_INITIALIZE_STRATEGY,
    ):
        return RunnerWireControl(
            kind=normalized, envelope=envelope, payload=payload_dict
        )

    return RunnerWireFrame(
        envelope=envelope,
        message_type=normalized,
        payload=payload_dict,
    )


def runner_response_envelope(
    envelope: WireEnvelope,
    *,
    response_type: str,
    message_id: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": response_type,
        **envelope.envelope_dict(),
    }
    if message_id is not None:
        out["message_id"] = message_id
    if envelope.message_id:
        out["correlation_id"] = envelope.message_id
    return out


def encode_health_response(envelope: WireEnvelope) -> str:
    body = runner_response_envelope(
        envelope,
        response_type=MESSAGE_TYPE_WORKER_HEALTH,
        message_id="worker_health",
    )
    body["payload"] = {"status": "UP"}
    return encode_jsonl_line(body)


def encode_strategy_initialized(
    envelope: WireEnvelope, payload: Mapping[str, Any]
) -> str:
    body = runner_response_envelope(
        envelope,
        response_type=MESSAGE_TYPE_STRATEGY_INITIALIZED,
        message_id="strategy_initialized",
    )
    body["payload"] = dict(payload)
    return encode_jsonl_line(body)


def encode_shutdown_ack(envelope: WireEnvelope) -> str:
    body = runner_response_envelope(
        envelope,
        response_type=MESSAGE_TYPE_SHUTDOWN_ACK,
        message_id="shutdown_ack",
    )
    body["payload"] = {}
    return encode_jsonl_line(body)


def encode_strategy_error(
    envelope: WireEnvelope,
    *,
    code: str,
    message: str,
) -> str:
    body = runner_response_envelope(
        envelope,
        response_type=MESSAGE_TYPE_STRATEGY_ERROR,
        message_id="strategy_error",
    )
    body["payload"] = {"code": code.strip().upper(), "message": message}
    return encode_jsonl_line(body)


def encode_session_response(
    envelope: WireEnvelope,
    response: OutboundMessage,
) -> str:
    if isinstance(response, StrategyErrorMessage):
        return encode_strategy_error(
            envelope, code=response.code, message=response.message
        )

    if isinstance(response, OrderIntentsMessage):
        body = runner_response_envelope(
            envelope,
            response_type=MESSAGE_TYPE_ORDER_INTENTS,
            message_id="order_intents",
        )
        body["intents"] = list(response.intents)
        return encode_jsonl_line(body)

    body = runner_response_envelope(
        envelope,
        response_type=MESSAGE_TYPE_NO_OP,
        message_id="noop",
    )
    body["payload"] = {}
    return encode_jsonl_line(body)


def runner_frame_to_portfolio_message(
    frame: RunnerWireFrame,
) -> PortfolioSnapshotMessage:
    positions = frame.payload.get("positions")
    if positions is not None and not isinstance(positions, list):
        raise RunnerWireDecodeError(
            "PORTFOLIO_SNAPSHOT payload.positions must be an array"
        )
    return PortfolioSnapshotMessage(portfolio=dict(frame.payload))


def runner_frame_to_open_orders_message(
    frame: RunnerWireFrame,
) -> OpenOrdersSnapshotMessage:
    orders = frame.payload.get("orders")
    if orders is None:
        orders = []
    if not isinstance(orders, list):
        raise RunnerWireDecodeError(
            "OPEN_ORDERS_SNAPSHOT payload.orders must be an array"
        )
    return OpenOrdersSnapshotMessage(orders=list(orders))


def runner_frame_to_market_data_message(
    frame: RunnerWireFrame,
) -> MarketDataEventMessage:
    payload = frame.payload
    event: dict[str, Any] = {
        "instrument_id": payload.get("instrument_id"),
        "symbol": payload.get("symbol"),
        "bar": payload.get("bar"),
    }
    if frame.envelope.backtest_time:
        event["time"] = frame.envelope.backtest_time
    bar = event.get("bar")
    if not isinstance(bar, Mapping):
        raise RunnerWireDecodeError("MARKET_DATA_EVENT payload.bar must be an object")
    return MarketDataEventMessage(event=event)


def handle_runner_frame(
    session: BacktestRunnerSession,
    frame: RunnerWireFrame | RunnerWireControl,
) -> tuple[str | None, int | None]:
    if isinstance(frame, RunnerWireControl):
        if frame.kind == MESSAGE_TYPE_HEALTH_CHECK:
            return encode_health_response(frame.envelope), None
        if frame.kind == MESSAGE_TYPE_INITIALIZE_STRATEGY:
            return encode_strategy_initialized(frame.envelope, frame.payload), None
        if frame.kind == MESSAGE_TYPE_SHUTDOWN:
            return encode_shutdown_ack(frame.envelope), 0

    assert isinstance(frame, RunnerWireFrame)
    try:
        if frame.message_type == MESSAGE_TYPE_PORTFOLIO_SNAPSHOT:
            inbound = runner_frame_to_portfolio_message(frame)
        elif frame.message_type == MESSAGE_TYPE_OPEN_ORDERS_SNAPSHOT:
            inbound = runner_frame_to_open_orders_message(frame)
        elif frame.message_type == MESSAGE_TYPE_MARKET_DATA_EVENT:
            inbound = runner_frame_to_market_data_message(frame)
        else:
            raise RunnerWireDecodeError(
                f"unsupported replay frame: {frame.message_type}"
            )
    except RunnerWireDecodeError as exc:
        return (
            encode_strategy_error(
                frame.envelope,
                code="WORKER_PROTOCOL_ERROR",
                message=str(exc),
            ),
            None,
        )

    response = session.handle(inbound)
    if isinstance(response, StrategyErrorMessage):
        return (
            encode_strategy_error(
                frame.envelope, code=response.code, message=response.message
            ),
            None,
        )

    return encode_session_response(frame.envelope, response), None
