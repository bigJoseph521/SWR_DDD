from __future__ import annotations

import json
import re
from unittest.mock import MagicMock

import pytest

from runtime.infrastructure.observability import stdout_event as stdout_event_module
from runtime.infrastructure.observability.stdout_event import write_stdout_event

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def captured_stdout_lines(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    lines: list[str] = []
    logger = MagicMock()
    logger.log = lambda _level, message, *args, **kwargs: lines.append(str(message))
    monkeypatch.setattr(stdout_event_module, "_event_logger", logger)
    return lines


def test_write_stdout_event_prefixes_iso_timestamp(
    captured_stdout_lines: list[str],
) -> None:
    write_stdout_event(
        level="INFO",
        event_name="worker.bootstrap.succeeded",
        message="Bootstrap succeeded",
    )
    assert len(captured_stdout_lines) == 1
    plain = _ANSI_ESCAPE.sub("", captured_stdout_lines[0]).strip()
    assert plain.startswith("[")
    json_start = plain.find('{"level"')
    record = json.loads(plain[json_start:])
    assert record == {
        "level": "INFO",
        "event_name": "worker.bootstrap.succeeded",
        "message": "Bootstrap succeeded",
    }


def test_write_stdout_event_includes_reason_code_on_failure(
    captured_stdout_lines: list[str],
) -> None:
    write_stdout_event(
        level="ERROR",
        event_name="worker.bootstrap.failed",
        message="Bootstrap failed",
        reason_code="SWR_ENTRYPOINT_INVALID",
    )
    plain = _ANSI_ESCAPE.sub("", captured_stdout_lines[0]).strip()
    json_start = plain.find('{"level"')
    record = json.loads(plain[json_start:])
    assert record["reason_code"] == "SWR_ENTRYPOINT_INVALID"
