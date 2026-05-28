from __future__ import annotations

from runtime.infrastructure.grpc.control_plane_envelope_log import write_stdout_event


def test_control_plane_module_reexports_write_stdout_event() -> None:
    assert write_stdout_event is not None
