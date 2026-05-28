"""Fetch SDS ``GET .../runtime-context`` and build a strategy-bundle-shaped dict for ``load_settings``."""

from __future__ import annotations

import json
import os
import uuid
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from runtime.domain.launch_spec import LaunchSpecValidationError
from runtime.infrastructure.observability.stdout_event import write_stdout_event

STRATEGY_DEPLOYMENT_SERVICE_BASE_URL_ENV = "STRATEGY_DEPLOYMENT_SERVICE_BASE_URL"
DEPLOYMENT_ID_ENV = "DEPLOYMENT_ID"


def _http_get(
    url: str, headers: dict[str, str], *, timeout_seconds: float
) -> tuple[int, bytes]:
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            code = resp.getcode()
            return (int(code) if code is not None else 200), resp.read()
    except HTTPError as exc:
        body = exc.read()
        return exc.code, body


def _data_source_to_feed_list(raw: object) -> list[str]:
    """Map SDS ``data_type`` string (e.g. ``BAR``) to ``parameters.data_source`` feed names."""
    if isinstance(raw, list):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    if not isinstance(raw, str):
        return ["bars"]
    s = raw.strip().upper()
    if not s:
        return ["bars"]
    if "," in s:
        return [p.strip().lower() for p in s.split(",") if p.strip()]
    mapping = {
        "BAR": ["bars"],
        "BARS": ["bars"],
        "TRADE": ["trades"],
        "TRADES": ["trades"],
        "QUOTE": ["quotes"],
        "QUOTES": ["quotes"],
    }
    return mapping.get(s, ["bars"])


def worker_bundle_dict_from_runtime_context_response(
    rc: Mapping[str, Any],
    *,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Merge SDS :class:`DeploymentRuntimeContextResponseData` JSON with runtime identity from env.

    ``identity`` must include ``runtime_id``. ``strategy_version_id`` may come from the response
    (preferred when present) or from ``identity`` / ``SWR_STRATEGY_VERSION_ID``.
    """
    out: dict[str, Any] = dict(identity)

    sv_rc = str(rc.get("strategy_version_id") or "").strip()
    if sv_rc:
        out["strategy_version_id"] = sv_rc

    account_id = str(rc.get("account_id") or "").strip()
    if account_id:
        out["account_id"] = account_id

    mode = str(rc.get("mode") or "").strip()
    if mode:
        out["mode"] = mode

    entrypoint = str(rc.get("entrypoint") or "").strip()
    if entrypoint:
        out["entrypoint"] = entrypoint

    artifact_uri = str(rc.get("artifact_uri") or "").strip()
    if artifact_uri:
        out["artifact_uri"] = artifact_uri
        # out["artifact_uri"] = "strategy_bundle/sma_crossover.zip"

    digest = rc.get("artifact_digest")
    if isinstance(digest, str) and digest.strip():
        out["artifact_digest"] = digest.strip()

    corr = str(rc.get("correlation_id") or "").strip()
    if corr:
        out["correlation_id"] = corr

    job_id = str(rc.get("job_id") or "").strip()
    if job_id:
        out["job_id"] = job_id

    params: dict[str, Any] = {}
    sp = rc.get("strategy_params")
    if isinstance(sp, dict):
        params.update(sp)
        if sp:
            out["strategy_params"] = dict(sp)

    bt = str(rc.get("bar_timeframe") or "").strip()
    if bt:
        params["bar_timeframe"] = bt

    feeds = _data_source_to_feed_list(rc.get("data_source"))
    if feeds:
        params["data_source"] = feeds

    sym = rc.get("symbol")
    if isinstance(sym, str) and sym.strip():
        params["symbol"] = sym.strip()

    inst = str(rc.get("instrument_id") or "").strip()
    if inst:
        params["instrument_id"] = inst

    if params:
        out["parameters"] = params

    initial_cash = rc.get("initial_cash")
    if isinstance(initial_cash, dict):
        amount = initial_cash.get("amount")
        currency = initial_cash.get("currency")
        if amount is not None and currency is not None:
            out["initial_cash"] = {
                "amount": str(amount).strip(),
                "currency": str(currency).strip(),
            }

    return out


def _runtime_identity_from_env() -> dict[str, Any]:
    rid = str(
        os.environ.get("RUNTIME_ID", "") or os.environ.get("SWR_RUNTIME_ID", "")
    ).strip()
    sv = str(os.environ.get("SWR_STRATEGY_VERSION_ID", "")).strip()
    tenant = os.environ.get("SWR_TENANT_ID")
    tenant_s = "" if tenant is None else str(tenant).strip()

    la_raw = str(os.environ.get("SWR_LAUNCH_ATTEMPT", "")).strip()
    launch_attempt = 1
    if la_raw:
        try:
            launch_attempt = max(1, int(la_raw))
        except ValueError:
            launch_attempt = 1

    out: dict[str, Any] = {
        "runtime_id": rid,
        "strategy_version_id": sv,
        "tenant_id": tenant_s,
        "launch_attempt": launch_attempt,
    }
    return out


def fetch_bundle_from_deployment_runtime_context(
    *,
    base_url: str,
    deployment_id: str,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    """
    GET runtime-context and return a bundle dict for
    :func:`runtime.infrastructure.config.settings._settings_from_prepared`.

    Raises :class:`LaunchSpecValidationError` on missing ``runtime_id`` (env), missing
    ``strategy_version_id`` when absent from both env and response, transport failure, non-JSON,
    or non-success HTTP status.
    """
    safe_id = quote(deployment_id.strip(), safe="")
    url = f"{base_url.rstrip('/')}/internal/v1/deployments/{safe_id}/runtime-context"
    request_id = str(uuid.uuid4())
    headers: dict[str, str] = {
        "accept": "application/json",
        "x-request-id": request_id,
    }
    correlation = os.environ.get("SWR_CORRELATION_ID", "").strip()
    if correlation:
        headers["x-correlation-id"] = correlation

    write_stdout_event(
        level="INFO",
        event_name="worker.runtime_context.loading",
        message="Loading deployment runtime context from strategy-deployment-service",
    )

    try:
        status, body = _http_get(url, headers, timeout_seconds=timeout_seconds)
    except URLError as exc:
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_unreachable",
            field_errors={"deployment_runtime_context": repr(exc)},
        ) from exc
    except OSError as exc:
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_unreachable",
            field_errors={"deployment_runtime_context": repr(exc)},
        ) from exc

    if status != 200:
        snippet = body.decode("utf-8", errors="replace")[:2000]
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_http_error",
            field_errors={
                "deployment_runtime_context": f"HTTP {status}",
                "body": snippet,
            },
        )

    if not body.strip():
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_empty_body",
            field_errors={"deployment_runtime_context": "empty response body"},
        )

    try:
        parsed = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_invalid_json",
            field_errors={"deployment_runtime_context": str(exc)},
        ) from exc

    if not isinstance(parsed, dict):
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_invalid_json",
            field_errors={
                "deployment_runtime_context": "response must be a JSON object",
            },
        )

    identity = _runtime_identity_from_env()
    sv_from_rc = str(parsed.get("strategy_version_id") or "").strip()
    if sv_from_rc:
        identity = {**identity, "strategy_version_id": sv_from_rc}

    missing: dict[str, str] = {}
    if not str(identity.get("runtime_id") or "").strip():
        missing["runtime_id"] = (
            "set RUNTIME_ID or SWR_RUNTIME_ID when using deployment runtime-context bootstrap"
        )
    if not str(identity.get("strategy_version_id") or "").strip():
        missing["strategy_version_id"] = (
            "set SWR_STRATEGY_VERSION_ID or ensure deployment runtime-context includes strategy_version_id"
        )
    if missing:
        raise LaunchSpecValidationError(
            reason="deployment_runtime_context_missing_identity",
            field_errors=missing,
        )

    bundle = worker_bundle_dict_from_runtime_context_response(parsed, identity=identity)
    write_stdout_event(
        level="INFO",
        event_name="worker.runtime_context.loaded",
        message="Deployment runtime context loaded",
    )
    return bundle
