from __future__ import annotations

import json

from runtime.integration.portfolio_redis_feed import (
    parse_portfolio_balance_message,
    portfolio_update_channel,
    portfolio_update_partition,
)
from runtime.integration.market_data_partition import market_data_partition


def test_portfolio_update_partition_matches_job_id_crc32() -> None:
    job_id = "550e8400-e29b-41d4-a716-446655440000"
    assert portfolio_update_partition(job_id, 128) == market_data_partition(job_id, 128)


def test_portfolio_update_partition_trims_job_id() -> None:
    job = "550e8400-e29b-41d4-a716-446655440000"
    assert portfolio_update_partition(job, 128) == portfolio_update_partition(
        f"  {job}  ", 128
    )


def test_portfolio_update_channel() -> None:
    assert portfolio_update_channel("portfolio:update", partition=42) == (
        "portfolio:update:42"
    )


def test_parse_portfolio_balance_message_valid() -> None:
    payload = {
        "job_id": "job-1",
        "timestamp": "2026-05-18T12:00:00.000Z",
        "balance": {
            "cash_balance": "1000.50",
            "buying_power": "900",
            "equity": "1100",
        },
    }
    raw = json.dumps(payload).encode("utf-8")
    parsed = parse_portfolio_balance_message(raw)
    assert parsed == payload


def test_parse_portfolio_balance_message_invalid() -> None:
    assert parse_portfolio_balance_message(b"{not json") is None
    assert parse_portfolio_balance_message("") is None
