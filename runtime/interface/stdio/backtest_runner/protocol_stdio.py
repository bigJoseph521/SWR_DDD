"""Keep JSON Lines on the real stdout pipe; route strategy ``print()`` to stderr."""

from __future__ import annotations

import os
import sys
from typing import TextIO

_protocol_stdout: TextIO | None = None


def install_protocol_stdio() -> TextIO:
    """
    Dup the runner JSONL pipe, then point OS fd 1 at stderr.

    ``sys.stdout`` is reassigned to stderr for Python ``print()``. Protocol frames
    are written only through the dup'd fd so C extensions or libraries that write to
    fd 1 cannot corrupt backtest-runner JSONL on stdout.
    """
    global _protocol_stdout
    if _protocol_stdout is not None:
        return _protocol_stdout

    protocol_fd = os.dup(sys.stdout.fileno())
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr
    _protocol_stdout = os.fdopen(protocol_fd, "wb", buffering=0, closefd=True)
    return _protocol_stdout


def protocol_stdout() -> TextIO:
    """Protocol-only stdout (raises if :func:`install_protocol_stdio` was not called)."""
    if _protocol_stdout is None:
        raise RuntimeError(
            "protocol stdout not installed; call install_protocol_stdio() first"
        )
    return _protocol_stdout


def write_protocol_line(line: str) -> None:
    """Write one JSONL row to the runner pipe and flush (never via ``sys.stdout``)."""
    stream = protocol_stdout()
    payload = line if line.endswith("\n") else f"{line}\n"
    data = payload.encode("utf-8")
    stream.write(data)
    stream.flush()
    try:
        os.fsync(stream.fileno())
    except OSError:
        pass
