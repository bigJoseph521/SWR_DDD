from __future__ import annotations

from typing import Any, Protocol

from runtime.domain.model.strategy_artifact_spec import StrategyArtifactSpec


class StrategyLoaderPort(Protocol):
    def load_strategy(self, artifact: StrategyArtifactSpec) -> Any: ...
