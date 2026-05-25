from __future__ import annotations

from types import SimpleNamespace

import pytest
from alphovex_sdk.strategy import Strategy as AlphovexStrategy
from runtime.bootstrap.entrypoint_loader import EntrypointLoadResult
from runtime.bootstrap.failures import (
    BootstrapStage,
    SDKContractFailure,
)
from runtime.bootstrap.sdk_contract_validator import (
    SdkContractValidator,
)


def _entrypoint(
    symbol: object,
    spec: str = "strategy.main:Strategy",
    *,
    details: dict[str, object] | None = None,
) -> EntrypointLoadResult:
    module_name, symbol_name = spec.split(":")
    return EntrypointLoadResult(
        entrypoint_spec=spec,
        module_name=module_name,
        symbol_name=symbol_name,
        symbol=symbol,
        details=details or {},
    )


def test_valid_sdk_compatible_strategy_passes() -> None:
    class Strategy(AlphovexStrategy):
        pass

    result = SdkContractValidator().validate(_entrypoint(Strategy))

    assert result.is_valid is True
    assert result.validated_type == "Strategy"


def test_missing_required_methods_fails() -> None:
    class StrategyMissingRun:
        def on_tick(self, context: object) -> None:
            return None

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator(
            required_base_type=None, required_methods=("run",)
        ).validate(_entrypoint(StrategyMissingRun))

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_MISSING_REQUIRED_METHOD"


def test_invalid_method_signature_fails() -> None:
    def run() -> None:
        return None

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator(
            required_base_type=None, required_methods=("run",)
        ).validate(_entrypoint(run, spec="strategy.main:run"))

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_INVALID_SIGNATURE"


def test_bare_callable_rejected_when_strategy_base_enforced() -> None:
    def run(ctx: object) -> object:
        return ctx

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(_entrypoint(run, spec="strategy.main:run"))

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_PROTOCOL_MISMATCH"


def test_non_strategy_class_rejected() -> None:
    class LegacyStrategy:
        def run(self, context: object) -> object:
            return context

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(_entrypoint(LegacyStrategy))

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_PROTOCOL_MISMATCH"


def test_protocol_base_mismatch_fails() -> None:
    class StrategyBase:
        def run(self, context: object) -> object:
            return context

    class NotStrategy:
        def run(self, context: object) -> object:
            return context

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator(required_base_type=StrategyBase).validate(
            _entrypoint(NotStrategy)
        )

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_PROTOCOL_MISMATCH"


def test_incompatible_sdk_marker_fails() -> None:
    class Strategy(AlphovexStrategy):
        __strategy_sdk_version__ = "2.1"

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(_entrypoint(Strategy))

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_MARKER_INCOMPATIBLE"


def test_mypy_failure_maps_to_sdk_protocol_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n"
        "\n"
        "class Strategy(_SdkStrategy):\n"
        "    pass\n",
        encoding="utf-8",
    )

    class Strategy(AlphovexStrategy):
        pass

    monkeypatch.setattr(
        "runtime.bootstrap.sdk_contract_validator.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="mypy error", stderr=""
        ),
    )

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(
            _entrypoint(Strategy, details={"module_file": str(strategy_file)})
        )

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_PROTOCOL_MISMATCH"
    assert failure.details.get("mypy_exit_code") == 1


def test_sdk_marker_failure_keeps_mypy_results_in_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n"
        "\n"
        "class Strategy(_SdkStrategy):\n"
        "    __strategy_sdk_version__ = '2.0'\n",
        encoding="utf-8",
    )

    class Strategy(AlphovexStrategy):
        __strategy_sdk_version__ = "2.0"

    monkeypatch.setattr(
        "runtime.bootstrap.sdk_contract_validator.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="Success: no issues found\n", stderr=""
        ),
    )

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(
            _entrypoint(Strategy, details={"module_file": str(strategy_file)})
        )

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_MARKER_INCOMPATIBLE"
    assert "mypy_result_path" in failure.details
    assert failure.details.get("mypy_exit_code") == 0


def test_mypy_failure_takes_precedence_over_sdk_marker_incompatible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n"
        "\n"
        "class Strategy(_SdkStrategy):\n"
        "    __strategy_sdk_version__ = '2.0'\n",
        encoding="utf-8",
    )

    class Strategy(AlphovexStrategy):
        __strategy_sdk_version__ = "2.0"

    monkeypatch.setattr(
        "runtime.bootstrap.sdk_contract_validator.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="mypy error", stderr=""
        ),
    )

    with pytest.raises(SDKContractFailure) as exc_info:
        SdkContractValidator().validate(
            _entrypoint(Strategy, details={"module_file": str(strategy_file)})
        )

    failure = exc_info.value
    assert failure.stage is BootstrapStage.SDK_VALIDATE
    assert failure.reason_code == "SDK_PROTOCOL_MISMATCH"
    assert failure.details.get("mypy_exit_code") == 1
