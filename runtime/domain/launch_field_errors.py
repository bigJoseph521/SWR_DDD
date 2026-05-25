"""Classify launch metadata field error codes into missed/invalid/empty buckets."""

from __future__ import annotations

from typing import Any, Mapping


def derive_field_buckets_from_field_errors(
    field_errors: Mapping[str, Any],
) -> tuple[list[str], list[str], list[str]]:
    missed: set[str] = set()
    invalid: set[str] = set()
    empty: set[str] = set()
    for field, code_raw in field_errors.items():
        key = str(field)
        code = str(code_raw)
        lower = code.lower()
        if key == "payload" and code.startswith("unknown_fields:"):
            invalid.update([x for x in code.split(":", 1)[1].split(",") if x])
            continue
        if "required_field_missing" in lower or "required" in lower:
            missed.add(key)
            continue
        if (
            "must_not_be_empty" in lower
            or "must_not_be_blank" in lower
            or "blank" in lower
        ):
            empty.add(key)
            continue
        invalid.add(key)
    return (sorted(missed), sorted(invalid), sorted(empty))
