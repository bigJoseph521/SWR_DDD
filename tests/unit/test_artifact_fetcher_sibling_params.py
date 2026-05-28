from __future__ import annotations

from pathlib import Path

from runtime.infrastructure.strategy_loader.artifact_fetcher import (
    LocalArtifactProvider,
)


def test_single_file_materialize_copies_adjacent_params_yaml(tmp_path: Path) -> None:
    src_dir = tmp_path / "bundle"
    src_dir.mkdir()
    strat = src_dir / "sma_crossover.py"
    strat.write_text("# strategy\n", encoding="utf-8")
    yaml_f = src_dir / "sma_crossover.yaml"
    yaml_f.write_text("indicator_params:\n  x:\n    default: 1\n", encoding="utf-8")

    dest = tmp_path / "materialized"
    ref = strat.resolve().as_uri()
    mat = LocalArtifactProvider().fetch(artifact_reference=ref, target_dir=dest)
    assert mat.digest_relative_paths == frozenset(
        {"sma_crossover.yaml", "sma_crossover.py"}
    )

    assert (dest / "sma_crossover.py").is_file()
    assert (dest / "sma_crossover.yaml").is_file()
    assert "indicator_params" in (dest / "sma_crossover.yaml").read_text(
        encoding="utf-8"
    )


def test_single_file_without_params_yaml_digest_scope_is_primary_only(
    tmp_path: Path,
) -> None:
    src_dir = tmp_path / "bundle"
    src_dir.mkdir()
    strat = src_dir / "only.py"
    strat.write_text("#\n", encoding="utf-8")
    dest = tmp_path / "materialized"
    mat = LocalArtifactProvider().fetch(
        artifact_reference=strat.resolve().as_uri(), target_dir=dest
    )
    assert mat.digest_relative_paths == frozenset({"only.py"})
    assert (dest / "only.py").is_file()
    assert not (dest / "params.yaml").exists()
