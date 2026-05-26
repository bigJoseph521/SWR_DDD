from __future__ import annotations

import json
import zlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from runtime.application.event_handling.portfolio_update_contract import (
    parse_decimal_balance_value,
    parse_portfolio_update_message,
    portfolio_update_channel_name,
    portfolio_update_partition,
)
from runtime.application.event_handling.portfolio_update_handler import (
    PortfolioUpdateHandler,
)
from runtime.application.event_handling.sdk_account_context_updater import (
    SdkAccountContextUpdater,
)
from runtime.infrastructure.strategy_loader.runtime_stub_support import (
    CashBalance,
    Exposure,
    MarginState,
    PnL,
    PortfolioSnapshot,
    SnapshotPortfolioService,
)
from runtime.domain.model.normalized_events import PortfolioUpdatedEvent
from runtime.infrastructure.redis.redis_portfolio_update_adapter import (
    RedisPortfolioUpdateAdapter,
)
from runtime.infrastructure.sdk.runtime_account_context import RuntimeAccountContext


JOB_ID = "550e8400-e29b-41d4-a716-446655440000"
PARTITION_COUNT = 128


def _valid_payload(
    *,
    job_id: str = JOB_ID,
    cash: str = "100000",
    buying_power: str = "95000",
    equity: str = "105250",
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "timestamp": "2026-05-18T14:30:00.000Z",
        "balance": {
            "cash_balance": cash,
            "buying_power": buying_power,
            "equity": equity,
        },
    }


def test_partition_calculation_uses_zlib_crc32() -> None:
    expected = zlib.crc32(JOB_ID.encode("utf-8")) % PARTITION_COUNT
    assert portfolio_update_partition(JOB_ID, PARTITION_COUNT) == expected


def test_job_id_routes_to_expected_decimal_partition() -> None:
    partition = portfolio_update_partition(JOB_ID, PARTITION_COUNT)
    assert 0 <= partition < PARTITION_COUNT
    crc = zlib.crc32(JOB_ID.encode("utf-8")) & 0xFFFFFFFF
    assert partition == crc % PARTITION_COUNT


def test_channel_name_is_portfolio_update_partition() -> None:
    partition = portfolio_update_partition(JOB_ID, PARTITION_COUNT)
    channel = portfolio_update_channel_name("portfolio:update", partition=partition)
    assert channel == f"portfolio:update:{partition}"


def test_matching_job_id_updates_runtime_context() -> None:
    snap = PortfolioSnapshot(
        account_id="acct-1",
        run_id="run-1",
        ts_event=datetime(2026, 1, 1, tzinfo=timezone.utc),
        cash_balance=CashBalance(currency="USD", free=100.0, locked=0.0),
        pnl=PnL(),
        exposure=Exposure(),
        margin_state=MarginState(),
        positions={},
    )
    svc = SnapshotPortfolioService(snap)
    account = RuntimeAccountContext(portfolio_service=svc)
    handler = PortfolioUpdateHandler(
        context_updater=SdkAccountContextUpdater(account),
    )

    parsed = parse_portfolio_update_message(
        _valid_payload(), expected_job_id=JOB_ID
    )
    assert parsed.event is not None
    handler.handle(parsed.event)

    portfolio = account.portfolio()
    assert portfolio.balance.cash_balance == 100000.0
    assert portfolio.balance.buying_power == 95000.0
    assert portfolio.balance.equity == 105250.0


def test_different_job_id_is_ignored() -> None:
    result = parse_portfolio_update_message(
        _valid_payload(job_id="other-job-id"),
        expected_job_id=JOB_ID,
    )
    assert result.event is None
    assert result.reject_reason is None


def test_balance_values_parsed_as_decimal_not_float() -> None:
    payload = _valid_payload(cash="100000.55")
    result = parse_portfolio_update_message(payload, expected_job_id=JOB_ID)
    assert result.event is not None
    assert isinstance(result.event.cash_balance, Decimal)
    assert result.event.cash_balance == Decimal("100000.55")
    assert not isinstance(result.event.cash_balance, float)


def test_float_balance_field_rejected() -> None:
    payload = _valid_payload()
    payload["balance"]["cash_balance"] = 100000.0
    result = parse_portfolio_update_message(payload, expected_job_id=JOB_ID)
    assert result.event is None
    assert result.reject_reason == "balance.cash_balance_required"


@pytest.mark.parametrize(
    "mutator,expected_reason",
    [
        (lambda p: p.pop("job_id"), "job_id_required"),
        (lambda p: p.pop("timestamp"), "timestamp_required"),
        (lambda p: p.pop("balance"), "balance_required"),
        (lambda p: p["balance"].pop("cash_balance"), "balance.cash_balance_required"),
        (lambda p: p["balance"].pop("buying_power"), "balance.buying_power_required"),
        (lambda p: p["balance"].pop("equity"), "balance.equity_required"),
    ],
)
def test_malformed_payload_rejected_without_crash(
    mutator: Any, expected_reason: str
) -> None:
    payload = _valid_payload()
    mutator(payload)
    rejected: list[str] = []
    emitted: list[PortfolioUpdatedEvent] = []

    def _on_reject(reason: str, _payload: dict[str, Any]) -> None:
        rejected.append(reason)

    adapter = RedisPortfolioUpdateAdapter(
        redis_url="redis://127.0.0.1:9",
        job_id=JOB_ID,
        partition_count=PARTITION_COUNT,
        on_event=emitted.append,
        on_rejected=_on_reject,
    )
    adapter.handle_parsed_message(payload)
    assert rejected == [expected_reason]
    assert emitted == []


def test_adapter_emits_normalized_event_for_matching_job() -> None:
    events: list[PortfolioUpdatedEvent] = []
    adapter = RedisPortfolioUpdateAdapter(
        redis_url="redis://127.0.0.1:9",
        job_id=JOB_ID,
        partition_count=PARTITION_COUNT,
        on_event=events.append,
    )
    adapter.handle_wire_message(json.dumps(_valid_payload()).encode("utf-8"))
    assert len(events) == 1
    assert events[0].job_id == JOB_ID
    assert events[0].cash_balance == Decimal("100000")


def test_parse_decimal_balance_value_rejects_float() -> None:
    assert parse_decimal_balance_value(1.5, field_name="x") is None
    assert parse_decimal_balance_value("1.5", field_name="x") == Decimal("1.5")
