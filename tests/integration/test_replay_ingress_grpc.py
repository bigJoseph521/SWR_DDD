from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping

import grpc
from runtime.interface.grpc.replay_ingress_server import (
    build_replay_ingress_server,
)
from runtime.infrastructure.grpc.serializers import (
    replay_worker_pb2,
    replay_worker_pb2_grpc,
)


class _ReplayIngressApp:
    def __init__(self) -> None:
        self.calls: list[Mapping[str, object]] = []

    def push_replay_context(
        self, payload: Mapping[str, object]
    ) -> Mapping[str, object]:
        self.calls.append(payload)
        replay_meta = payload.get("replay")
        replay_meta_dict = dict(replay_meta) if isinstance(replay_meta, dict) else {}
        events = payload.get("events")
        consumed_count = len(events) if isinstance(events, list) else 0
        return {
            "runtime_id": "rt-backtest-1",
            "replay_session_id": str(replay_meta_dict.get("replay_session_id") or ""),
            "replay_cursor": str(replay_meta_dict.get("replay_cursor") or ""),
            "consumed_count": consumed_count,
            "observed_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        }


def test_replay_ingress_push_context_calls_application_and_returns_ack() -> None:
    app = _ReplayIngressApp()
    runtime = build_replay_ingress_server(application=app, bind_address="127.0.0.1:0")
    runtime.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{runtime.port}")
    stub = replay_worker_pb2_grpc.ReplayIngressServiceStub(channel)

    request = replay_worker_pb2.ReplayContext(
        replay=replay_worker_pb2.ReplayMetadata(
            meta=replay_worker_pb2.CommonMetadata(
                runtime_id="rt-backtest-1",
                launch_attempt=1,
                strategy_version_id="sv-1",
            ),
            replay_session_id="sess-1",
            replay_cursor="cur-1",
        ),
        events=[
            replay_worker_pb2.ReplayEvent(
                event_id="evt-1",
                event_type="market.bar",
                instrument_id="AAPL",
                sequence=1,
            )
        ],
        end_of_stream=False,
    )
    response = stub.PushReplayContext(request)

    assert response.runtime_id == "rt-backtest-1"
    assert response.replay_session_id == "sess-1"
    assert response.replay_cursor == "cur-1"
    assert response.consumed_count == 1
    assert len(app.calls) == 1

    channel.close()
    runtime.stop(0)
