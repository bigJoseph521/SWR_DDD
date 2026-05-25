from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StrategyArtifactSpec:
    """Inputs for strategy loading only (not strategy calculation)."""

    strategy_uri: str
    artifact_digest: str | None
    entrypoint: str
