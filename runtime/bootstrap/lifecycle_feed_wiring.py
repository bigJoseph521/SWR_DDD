from __future__ import annotations

import os
import threading
import uuid

from runtime.application.ports.worker_domain_events import StrategyWorkerDomainEvent
from runtime.domain.enums import WorkerMode


def paper_live_symbol_allowlist(svc: object) -> set[str] | None:
    params = svc._launch_payload.get("parameters")
    if not isinstance(params, dict):
        return None
    sym = params.get("symbol")
    if isinstance(sym, str) and sym.strip():
        return {sym.strip().upper()}
        # return {"TSM"}
    return None


def paper_live_strategy_symbol(svc: object) -> str | None:
    allow = paper_live_symbol_allowlist(svc)
    if not allow:
        return None
    return next(iter(allow))


def live_market_data_partition_scope(
    svc: object, *, partition_count: int
) -> tuple[str, int, set[str]] | None:
    """
    Resolve strategy symbol and its single Redis stream partition for XREAD.

    Returns ``(symbol_upper, partition, {symbol_upper})`` or ``None`` when
    ``parameters.symbol`` is missing.
    """
    from runtime.infrastructure.redis.market_data_partition import market_data_partition

    sym = paper_live_strategy_symbol(svc)
    if not sym:
        print(
            "[market-data-redis] parameters.symbol is required for PAPER/LIVE "
            "market data (single-partition XREAD); not opening all partitions.",
            flush=True,
        )
        return None
    part = market_data_partition(sym, partition_count)
    return sym, part, {sym}


def maybe_start_market_data_feed(svc: object) -> None:
    if svc._live_md_feed_started or svc._backtest_stdin_feed_started:
        return
    if svc._launch_spec.mode is WorkerMode.BACKTEST:
        maybe_start_backtest_stdin_market_data_feed(svc)
        return
    if svc._launch_spec.mode not in (WorkerMode.PAPER, WorkerMode.LIVE):
        return
    wrs = svc._worker_runtime_settings
    redis_url = (
        str(getattr(wrs, "market_data_redis_url", "") or "").strip() if wrs else ""
    )
    if not redis_url:
        print(
            "[market-data-redis] not configured (set market_data_redis_url or "
            "SWR_MARKET_DATA_REDIS_URL); PAPER/LIVE worker will idle until a feed is available.",
            flush=True,
        )
        return
    from runtime.infrastructure.redis.market_data_redis_feed import (
        build_market_data_consumer_group_name,
        expand_market_data_stream_keys,
        market_data_read_command_fields,
        market_data_redis_keys_from_settings,
        redis_stream_key_prefixes_snapshot,
        resolve_stream_names,
        sanitize_redis_url_for_log,
    )

    feeds = tuple(getattr(wrs, "market_data_feeds", ()) or ("bars",))
    bar_tf = (getattr(wrs, "replay_bar_timeframe", None) or "1m").strip() or "1m"
    md_keys = market_data_redis_keys_from_settings(wrs)
    svc._market_data_stream_log(
        level="INFO",
        event_name="market_data_stream.step_resolve_stream_prefixes",
        message="Step: stream prefixes from env/bundle (XREAD on md:stream:*; not Pub/Sub md:realtime:*).",
        fields={
            "bar_timeframe": bar_tf,
            "feeds": list(feeds),
            **redis_stream_key_prefixes_snapshot(md_keys),
        },
    )
    stream_bases = resolve_stream_names(feeds, bar_timeframe=bar_tf, redis_keys=md_keys)
    svc._market_data_stream_log(
        level="INFO",
        event_name="market_data_stream.step_resolve_stream_bases",
        message="Step: logical stream base keys for selected feeds (before :partition suffix).",
        fields={
            "stream_bases": list(stream_bases),
        },
    )
    pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
    try:
        partition_count = int(pc_raw)
    except (TypeError, ValueError):
        partition_count = 128
    scope = live_market_data_partition_scope(svc, partition_count=partition_count)
    if scope is None:
        return
    strategy_symbol, partition, partition_symbols = scope
    stream_names = expand_market_data_stream_keys(
        stream_bases,
        partition_count=partition_count,
        symbol_filter=partition_symbols,
    )
    if not stream_names:
        print(
            "[market-data-redis] market_data_feeds produced no stream keys; "
            "check parameters.data_source / market_data_streams / SWR_MARKET_DATA_STREAMS "
            "and bar_timeframe (Redis bars are 1m / md:stream:am only).",
            flush=True,
        )
        return
    svc._market_data_stream_log(
        level="INFO",
        event_name="market_data_stream.step_expand_partitioned_streams",
        message="Step: physical Redis stream keys for XREAD / XREADGROUP (base:partition).",
        fields={
            "partition_count": partition_count,
            "strategy_symbol": strategy_symbol,
            "partition": partition,
            "physical_stream_keys": list(stream_names),
            "physical_stream_key_count": len(stream_names),
        },
    )
    use_cg = bool(getattr(wrs, "market_data_redis_use_consumer_group", False))
    cgp = (
        str(
            getattr(wrs, "market_data_consumer_group_prefix", "strategy-worker-runtime")
            or "strategy-worker-runtime"
        ).strip()
        or "strategy-worker-runtime"
    )
    dep_raw = svc._launch_payload.get("deployment_id")
    dep_s = str(dep_raw).strip() if dep_raw is not None else ""
    cg_preview = (
        build_market_data_consumer_group_name(
            group_prefix=cgp,
            deployment_id=dep_s or None,
            runtime_id=svc._launch_spec.runtime_id,
        )[:200]
        if use_cg
        else ""
    )
    cmd_preview = market_data_read_command_fields(
        use_consumer_group=use_cg,
        stream_names=list(stream_names),
        stream_start_id=str(getattr(wrs, "market_data_stream_start_id", "$") or "$"),
        block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
        count=int(getattr(wrs, "market_data_xread_count", 100)),
        consumer_group_name=cg_preview if use_cg else "",
        consumer_name="(assigned in swr-market-data-redis-feed thread)"
        if use_cg
        else "",
    )
    svc._market_data_stream_log(
        level="INFO",
        event_name="market_data_stream.subscription_snapshot",
        message="Resolved Redis stream subscription for market data ingress.",
        fields={
            "redis_url": sanitize_redis_url_for_log(redis_url),
            "feeds": list(feeds),
            "bar_timeframe": bar_tf,
            "strategy_symbol": strategy_symbol,
            "partition": partition,
            "partition_count": partition_count,
            "redis_stream_keys": list(stream_names),
            "use_consumer_group": use_cg,
            "consumer_group": cg_preview or None,
            **redis_stream_key_prefixes_snapshot(md_keys),
            **cmd_preview,
        },
    )
    svc._emit_domain_event(
        StrategyWorkerDomainEvent.MARKET_DATA_SUBSCRIPTION_STARTED,
        "Market data Redis subscription started",
        event_extras={
            "stream_count": len(stream_names),
            "feeds": list(feeds),
            "stage": "market_data_subscription",
            "state": "started",
        },
    )
    svc._live_md_feed_started = True
    svc._live_md_feed_stop.clear()
    thread = threading.Thread(
        target=lambda: live_market_data_redis_worker(svc),
        name="swr-market-data-redis-feed",
        daemon=False,
    )
    svc._live_md_feed_thread = thread
    thread.start()


def stop_market_data_feed(svc: object) -> None:
    stop_backtest_stdin_market_data_feed(svc)
    svc._live_md_feed_stop.set()
    t = svc._live_md_feed_thread
    if t is not None and t.is_alive():
        t.join(timeout=60.0)


def maybe_start_backtest_stdin_market_data_feed(svc: object) -> None:
    """Start BACKTEST stdin market-data pull loop (placeholder wiring)."""
    if svc._backtest_stdin_feed_started:
        return
    if svc._launch_spec.mode is not WorkerMode.BACKTEST:
        return
    from runtime.interface.stdio.backtest_stdin_market_data_feed import (
        BacktestStdinMarketDataFeed,
    )

    feed = BacktestStdinMarketDataFeed()
    svc._backtest_stdin_feed = feed
    svc._backtest_stdin_feed_started = True
    feed.start(
        svc._dispatch_backtest_stdin_tick,
        on_end_of_stream=svc.mark_backtest_market_data_stream_complete,
    )
    import sys

    print(
        "[backtest-stdin] market-data feed started (stdin JSON lines; payload TBD)",
        file=sys.stderr,
        flush=True,
    )


def stop_backtest_stdin_market_data_feed(svc: object) -> None:
    feed = getattr(svc, "_backtest_stdin_feed", None)
    if feed is not None:
        feed.stop()
    svc._backtest_stdin_feed = None
    svc._backtest_stdin_feed_started = False


def live_market_data_redis_worker(svc: object) -> None:
    from runtime.infrastructure.redis.market_data_redis_feed import (
        build_market_data_consumer_group_name,
        expand_market_data_stream_keys,
        market_data_read_command_fields,
        market_data_redis_keys_from_settings,
        redis_stream_key_prefixes_snapshot,
        resolve_stream_names,
        run_market_data_redis_loop,
        sanitize_redis_stream_consumer_token,
        sanitize_redis_url_for_log,
    )

    wrs = svc._worker_runtime_settings
    if wrs is None:
        return
    url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
    if not url:
        return
    feeds = tuple(getattr(wrs, "market_data_feeds", ()) or ("bars",))
    bar_tf = (getattr(wrs, "replay_bar_timeframe", None) or "1m").strip() or "1m"
    md_keys = market_data_redis_keys_from_settings(wrs)
    stream_bases = resolve_stream_names(feeds, bar_timeframe=bar_tf, redis_keys=md_keys)
    if not stream_bases:
        return
    pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
    try:
        partition_count = int(pc_raw)
    except (TypeError, ValueError):
        partition_count = 128
    scope = live_market_data_partition_scope(svc, partition_count=partition_count)
    if scope is None:
        return
    strategy_symbol, partition, partition_symbols = scope
    stream_names = expand_market_data_stream_keys(
        stream_bases,
        partition_count=partition_count,
        symbol_filter=partition_symbols,
    )
    if not stream_names:
        return
    use_cg = bool(getattr(wrs, "market_data_redis_use_consumer_group", False))
    cgp = (
        str(
            getattr(wrs, "market_data_consumer_group_prefix", "strategy-worker-runtime")
            or "strategy-worker-runtime"
        ).strip()
        or "strategy-worker-runtime"
    )
    cnp = (
        str(
            getattr(
                wrs,
                "market_data_consumer_name_prefix",
                "strategy-worker-runtime-worker",
            )
            or "strategy-worker-runtime-worker"
        ).strip()
        or "strategy-worker-runtime-worker"
    )
    dep_raw = svc._launch_payload.get("deployment_id")
    dep_s = str(dep_raw).strip() if dep_raw is not None else ""
    group = build_market_data_consumer_group_name(
        group_prefix=cgp,
        deployment_id=dep_s or None,
        runtime_id=svc._launch_spec.runtime_id,
    )[:200]
    consumer = (
        f"{sanitize_redis_stream_consumer_token(cnp)}-"
        f"{os.getpid()}-{uuid.uuid4().hex[:10]}"
    )
    svc._market_data_stream_log(
        level="INFO",
        event_name="market_data_stream.step_worker_thread_ingress",
        message="Step: worker thread starting Redis ingress (same plan as subscription_snapshot).",
        fields={
            "redis_url": sanitize_redis_url_for_log(url),
            "feeds": list(feeds),
            "bar_timeframe": bar_tf,
            "strategy_symbol": strategy_symbol,
            "partition": partition,
            "partition_count": partition_count,
            "physical_stream_keys": list(stream_names),
            "use_consumer_group": use_cg,
            "consumer_group": group if use_cg else None,
            "consumer_name": consumer if use_cg else None,
            **redis_stream_key_prefixes_snapshot(md_keys),
            **market_data_read_command_fields(
                use_consumer_group=use_cg,
                stream_names=list(stream_names),
                stream_start_id=str(
                    getattr(wrs, "market_data_stream_start_id", "$") or "$"
                ),
                block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
                count=int(getattr(wrs, "market_data_xread_count", 100)),
                consumer_group_name=group if use_cg else "",
                consumer_name=consumer if use_cg else "",
            ),
        },
    )
    run_market_data_redis_loop(
        redis_url=url,
        stream_names=stream_names,
        stream_start_id=str(getattr(wrs, "market_data_stream_start_id", "$") or "$"),
        block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
        count=int(getattr(wrs, "market_data_xread_count", 100)),
        strategy_symbol=strategy_symbol,
        on_tick=svc._dispatch_paper_live_tick,
        should_stop=svc._live_md_feed_stop,
        bar_timeframe=bar_tf,
        redis_keys=md_keys,
        use_consumer_group=use_cg,
        consumer_group_name=group if use_cg else "",
        consumer_name=consumer if use_cg else "",
        log=lambda **kw: svc._market_data_stream_log(
            level=str(kw.get("level") or "INFO"),
            event_name=str(kw.get("event_name") or ""),
            message=str(kw.get("message") or ""),
            fields=dict(kw.get("fields") or {}),
        ),
    )


def maybe_start_portfolio_update_feed(svc: object) -> None:
    if svc._portfolio_update_feed_started:
        return
    if svc._launch_spec.mode not in (WorkerMode.PAPER, WorkerMode.LIVE):
        return
    wrs = svc._worker_runtime_settings
    if wrs is None or not bool(getattr(wrs, "portfolio_update_enabled", True)):
        return
    job_id = str(svc._launch_spec.job_id or "").strip()
    if not job_id:
        print(
            "[portfolio-update-redis] launch job_id is required for portfolio "
            "balance subscription; feed not started.",
            flush=True,
        )
        return
    redis_url = str(getattr(wrs, "portfolio_update_redis_url", "") or "").strip()
    if not redis_url:
        redis_url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
    if not redis_url:
        print(
            "[portfolio-update-redis] not configured (set portfolio_update_redis_url, "
            "SWR_PORTFOLIO_UPDATE_REDIS_URL, or market_data_redis_url).",
            flush=True,
        )
        return

    svc._portfolio_update_feed_started = True
    svc._portfolio_update_feed_stop.clear()
    thread = threading.Thread(
        target=lambda: portfolio_update_redis_worker(svc),
        name="swr-portfolio-update-feed",
        daemon=False,
    )
    svc._portfolio_update_feed_thread = thread
    thread.start()


def stop_portfolio_update_feed(svc: object) -> None:
    svc._portfolio_update_feed_stop.set()
    t = svc._portfolio_update_feed_thread
    if t is not None and t.is_alive():
        t.join(timeout=60.0)


def portfolio_update_redis_worker(svc: object) -> None:
    from runtime.infrastructure.redis.redis_portfolio_update_adapter import (
        RedisPortfolioUpdateAdapter,
    )

    wrs = svc._worker_runtime_settings
    if wrs is None:
        return
    job_id = str(svc._launch_spec.job_id or "").strip()
    if not job_id:
        return
    redis_url = str(getattr(wrs, "portfolio_update_redis_url", "") or "").strip()
    if not redis_url:
        redis_url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
    if not redis_url:
        return
    pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
    try:
        partition_count = int(pc_raw)
    except (TypeError, ValueError):
        partition_count = 128
    prefix = str(
        getattr(wrs, "portfolio_update_channel_prefix", "portfolio:update")
        or "portfolio:update"
    ).strip()
    adapter = RedisPortfolioUpdateAdapter(
        redis_url=redis_url,
        job_id=job_id,
        partition_count=partition_count,
        channel_prefix=prefix,
        on_event=svc._dispatch_portfolio_updated_event,
        on_rejected=lambda reason, payload: svc._portfolio_update_stream_log(
            level="WARNING",
            event_name="portfolio_update_adapter.rejected",
            message=f"Portfolio update rejected: {reason}",
            fields={"reason": reason, "job_id": str(payload.get("job_id") or "")},
        ),
        log=lambda **kw: svc._portfolio_update_stream_log(
            level=str(kw.get("level") or "INFO"),
            event_name=str(kw.get("event_name") or ""),
            message=str(kw.get("message") or ""),
            fields=dict(kw.get("fields") or {}),
        ),
    )
    adapter.run_pubsub_loop(should_stop=svc._portfolio_update_feed_stop)
