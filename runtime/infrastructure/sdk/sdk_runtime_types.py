"""Runtime strategy metadata types not provided by the current alphovex_sdk layout."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


@dataclass
class ParameterSchema:
    """Parameter defaults/schema for replay :class:`RuntimeParamsContext`."""

    parameters: dict[str, Any] = field(default_factory=dict)


class AssetClass(StrEnum):
    EQUITY = "equity"
    FUTURE = "future"
    OPTION = "option"
    CRYPTO = "crypto"
    FX = "fx"


@dataclass
class StrategyMetadata:
    name: str
    description: str
    version: str
    author: str
    supported_asset_classes: tuple[AssetClass, ...]
    parameter_schema: ParameterSchema
