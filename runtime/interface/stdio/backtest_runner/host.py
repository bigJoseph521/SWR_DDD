"""Stdio JSON Lines host loop for backtest-runner subprocess integration."""

from __future__ import annotations

import json
import sys
import traceback
from typing import Mapping, TextIO

from runtime.application.backtest_runner.session import BacktestRunnerSession
from runtime.interface.stdio.backtest_runner.protocol_stdio import write_protocol_line
from runtime.interface.stdio.backtest_runner.wire.messages import WireEnvelope
from runtime.interface.stdio.backtest_runner.wire.runner_wire import (
    MESSAGE_TYPE_SHUTDOWN,
    RunnerWireControl,
    RunnerWireDecodeError,
    decode_runner_line,
    encode_strategy_error,
    handle_runner_frame,
)


def log_stderr(stream: TextIO, message: str) -> None:
    stream.write(message.rstrip("\n") + "\n")
    stream.flush()


def _try_decode_envelope_for_error(raw_line: str) -> WireEnvelope | None:
    trimmed = raw_line.strip()
    if not trimmed:
        return None
    try:
        raw = json.loads(trimmed)
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, Mapping):
        return None
    message_type = raw.get("type")
    if not isinstance(message_type, str):
        return None
    try:
        frame = decode_runner_line(trimmed)
    except RunnerWireDecodeError:
        return None
    if isinstance(frame, RunnerWireControl):
        return frame.envelope
    return frame.envelope


class BacktestRunnerStdioHost:
    """Reads backtest-runner JSONL from stdin and writes envelope responses to stdout."""

    def __init__(
        self,
        input_stream: TextIO | None = None,
        output_stream: TextIO | None = None,
        log_stream: TextIO | None = None,
        *,
        session: BacktestRunnerSession,
    ) -> None:
        self._input = input_stream if input_stream is not None else sys.stdin
        self._output = output_stream
        self._log = log_stream if log_stream is not None else sys.stderr
        self._session = session

    @property
    def session(self) -> BacktestRunnerSession:
        return self._session

    def _emit_line(self, line: str) -> None:
        if self._output is not None:
            self._output.write(line)
            self._output.flush()
        else:
            write_protocol_line(line)

    def run(self) -> int:
        log_stderr(
            self._log,
            "strategy-worker-runtime stdio-jsonl host started (backtest-runner wire)",
        )

        line_number = 0
        try:
            for raw_line in self._input:
                line_number += 1
                try:
                    frame = decode_runner_line(raw_line, line_number=line_number)
                except RunnerWireDecodeError as exc:
                    log_stderr(
                        self._log,
                        f"runner wire decode error (line {line_number}): {exc}",
                    )
                    envelope = _try_decode_envelope_for_error(raw_line)
                    if envelope is not None:
                        self._emit_line(
                            encode_strategy_error(
                                envelope,
                                code="WORKER_PROTOCOL_ERROR",
                                message=str(exc),
                            )
                        )
                    continue

                try:
                    line_out, exit_code = handle_runner_frame(self._session, frame)
                except Exception as exc:
                    log_stderr(
                        self._log,
                        f"runner frame handler failed (line {line_number}): {exc}",
                    )
                    traceback.print_exc(file=self._log)
                    if isinstance(frame, RunnerWireControl):
                        envelope = frame.envelope
                    else:
                        envelope = frame.envelope
                    self._emit_line(
                        encode_strategy_error(
                            envelope,
                            code="WORKER_INTERNAL_ERROR",
                            message=str(exc),
                        )
                    )
                    continue

                if line_out is not None:
                    self._emit_line(line_out)
                if exit_code is not None:
                    if (
                        isinstance(frame, RunnerWireControl)
                        and frame.kind == MESSAGE_TYPE_SHUTDOWN
                    ):
                        log_stderr(self._log, "sent SHUTDOWN_ACK; exiting")
                    return exit_code
        except Exception as exc:
            log_stderr(self._log, f"fatal stdio host error: {exc}")
            traceback.print_exc(file=self._log)
            return 1

        return 0
