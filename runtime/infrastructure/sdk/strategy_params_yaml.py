"""Load parameter specs from a strategy-adjacent YAML file (``{module_stem}.yaml``)."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Mapping

from runtime.infrastructure.sdk.sdk_runtime_types import ParameterSchema

_LEGACY_PARAMS_YAML = "params.yaml"


def params_yaml_filename_for_module(module_file: Path | str) -> str:
    """Basename for parameter defaults YAML (matches strategy zip name, e.g. ``sma_crossover.yaml``)."""
    return f"{Path(module_file).stem}.yaml"


def discover_params_yaml_adjacent_to_module_file(
    module_file: Path | str,
) -> Path | None:
    """
    Resolve parameter YAML next to a strategy module file.

    Prefers ``{module_stem}.yaml`` (same stem as the strategy ``.py`` / bundle ``.zip``).
    Falls back to legacy ``params.yaml`` when present.
    """
    parent = Path(module_file).resolve().parent
    stem_yaml = parent / params_yaml_filename_for_module(module_file)
    if stem_yaml.is_file():
        return stem_yaml
    legacy = parent / _LEGACY_PARAMS_YAML
    if legacy.is_file():
        return legacy
    return None


def _dict_specs_from_mapping_block(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = dict(value)
    return out


def _parameter_specs_from_yaml_mapping(data: Mapping[str, Any]) -> dict[str, Any]:
    """
    Prefer JSON Schema-style ``properties``; if empty or absent, use legacy ``indicator_params``.
    """
    props = _dict_specs_from_mapping_block(data.get("properties"))
    if props:
        return props
    return _dict_specs_from_mapping_block(data.get("indicator_params"))


def _warmup_param_specs_from_yaml_mapping(data: Mapping[str, Any]) -> dict[str, Any]:
    raw = data.get("warmup_params")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = dict(value)
    return out


def load_warmup_bars_default_from_params_yaml(path: Path) -> int:
    """
    Read ``warmup_params.warmup_bars.default`` from ``params.yaml`` when present.

    If ``warmup_bars`` is absent, falls back to ``warmup_params.warmup_lookback_days.default``.
    """
    try:
        import yaml
    except ImportError:
        return 0

    if not path.is_file():
        return 0

    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return 0

    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError:
        return 0

    if not isinstance(loaded, dict):
        return 0

    specs = _warmup_param_specs_from_yaml_mapping(loaded)
    wb = specs.get("warmup_bars")
    if isinstance(wb, dict):
        d = wb.get("default")
        if isinstance(d, int) and not isinstance(d, bool) and d > 0:
            return d
        if isinstance(d, str) and d.strip().isdigit():
            v = int(d.strip())
            return v if v > 0 else 0

    wl = specs.get("warmup_lookback_days")
    if isinstance(wl, dict):
        d = wl.get("default")
        if isinstance(d, int) and not isinstance(d, bool) and d > 0:
            return d
        if isinstance(d, str) and d.strip().isdigit():
            v = int(d.strip())
            return v if v > 0 else 0

    return 0


def load_warmup_bars_default_for_strategy(strategy: object) -> int:
    """Resolve warmup bar count from ``params.yaml`` adjacent to the strategy module, if any."""
    path = discover_adjacent_params_yaml(strategy)
    if path is None:
        return 0
    return load_warmup_bars_default_from_params_yaml(path)


def load_indicator_params_from_params_yaml(path: Path) -> dict[str, Any]:
    """
    Parse ``params.yaml`` and return flat parameter specs for :class:`ParameterSchema`.

    Reads JSON Schema-style ``properties`` when present and non-empty; otherwise falls back to
    legacy ``indicator_params``. Each parameter key maps to a dict (typically ``type`` /
    ``default`` / ``description``).
    """
    try:
        import yaml
    except ImportError:
        return {}

    if not path.is_file():
        return {}

    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError:
        return {}

    if not isinstance(loaded, dict):
        return {}

    return _parameter_specs_from_yaml_mapping(loaded)


def discover_adjacent_params_yaml(strategy: object) -> Path | None:
    """Return adjacent parameter YAML for the strategy class module, if it exists."""
    cls = type(strategy)
    try:
        module_file = inspect.getfile(cls)
    except (OSError, TypeError):
        return None
    return discover_params_yaml_adjacent_to_module_file(module_file)


def merge_parameter_schema_with_adjacent_params_yaml(
    strategy: object,
    schema: ParameterSchema,
) -> ParameterSchema:
    """
    Merge parameter specs from adjacent strategy YAML (``{stem}.yaml``) into ``schema``.

    Specs come from ``properties`` or legacy ``indicator_params`` (see
    :func:`load_indicator_params_from_params_yaml`). Keys from :meth:`build_parameter_schema`
    (if any) override YAML entries with the same name.
    """
    yaml_path = discover_adjacent_params_yaml(strategy)
    if yaml_path is None:
        return schema

    from_yaml = load_indicator_params_from_params_yaml(yaml_path)
    if not from_yaml:
        return schema

    merged: dict[str, Any] = dict(from_yaml)
    merged.update(schema.parameters)
    return ParameterSchema(parameters=merged)
