from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


class FakeManagerClient:
    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []

    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.signals.append(payload)
        return {"accepted": True, "signal_type": payload.get("signal_type")}


class FakeRiskOrderIntentClient:
    def __init__(self) -> None:
        self.intent_calls: list[dict[str, Any]] = []

    def submit_order_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.intent_calls.append(dict(payload))
        return {"accepted": True, "id": payload.get("idempotency_key")}


def write_strategy_package(
    base: Path,
    *,
    package_name: str = "strategy_pkg",
    sdk_marker: str = "1.0",
) -> Path:
    package_dir = base / package_name
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "main.py").write_text(
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n"
        "\n"
        "class Strategy(_SdkStrategy):\n"
        f"    __strategy_sdk_version__ = '{sdk_marker}'\n",
        encoding="utf-8",
    )
    return base


def digest_tree(root: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()), key=lambda p: str(p)
    ):
        hasher.update(path.relative_to(root).as_posix().encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def build_bundle_dict(
    *,
    tmp_path: Path,
    source_root: Path,
    runtime_id: str,
    mode: str = "PAPER",
    entrypoint: str = "strategy_pkg.main:Strategy",
    artifact_digest: str | None = None,
    launch_attempt: int = 1,
    job_id: str | None = None,
) -> dict[str, Any]:
    """
    In-memory ``setting.json``-shaped dict for :func:`load_settings_from_bundle_dict`.
    Pass ``tmp_path`` as ``base_dir=tmp_path`` when loading so ``work_root`` resolves correctly.
    """
    effective_digest = artifact_digest or f"sha256:{digest_tree(source_root)}"
    data: dict[str, Any] = {
        "runtime_id": runtime_id,
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": mode,
        "artifact_uri": source_root.resolve().as_uri(),
        "artifact_digest": effective_digest,
        "entrypoint": entrypoint,
        "launch_attempt": launch_attempt,
        "work_root": "work",
    }
    if job_id is not None:
        data["job_id"] = job_id
    if mode == "BACKTEST":
        data.setdefault("ts_start", "2026-01-01T00:00:00Z")
        data.setdefault("ts_end", "2026-01-02T00:00:00Z")
    _ = tmp_path
    return data
