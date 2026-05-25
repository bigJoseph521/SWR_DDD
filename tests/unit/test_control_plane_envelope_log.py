from __future__ import annotations

import json

from runtime.transport.grpc.control_plane_envelope_log import (
    BANNER_WR_TO_RM,
    write_control_plane_envelope,
)


def test_write_control_plane_envelope_prints_json_to_stdout(
    capsys,
) -> None:
    write_control_plane_envelope(
        banner=BANNER_WR_TO_RM,
        message={"event_name": "runtime.heartbeat", "payload": {"x": 1}},
    )

    out = capsys.readouterr().out
    assert BANNER_WR_TO_RM in out
    payload_line = out.split("\n", 2)[1]
    _, json_part = payload_line.split(": ", 1)
    stored = json.loads(json_part)
    assert stored["event_name"] == "runtime.heartbeat"
    assert stored["payload"]["x"] == 1
