"""Bootstrap pipeline stage identifiers (application-owned; no bootstrap import)."""

from __future__ import annotations

from runtime.domain.errors import RuntimeStartValidationFailedError

BOOTSTRAP_STAGE_ORDER: tuple[str, ...] = (
    "ARTIFACT_FETCH",
    "ARTIFACT_VERIFY",
    "ENTRYPOINT_LOAD",
    "SDK_VALIDATE",
)


def classify_bootstrap_stages(
    error: Exception, startup_steps: list[str]
) -> tuple[list[str], list[str], list[str]]:
    if isinstance(error, RuntimeStartValidationFailedError):
        return ([], ["validate_launch_metadata"], list(BOOTSTRAP_STAGE_ORDER))

    stage = getattr(error, "stage", None)
    stage_value = str(getattr(stage, "value", stage) or "")
    if stage_value in BOOTSTRAP_STAGE_ORDER:
        failed_index = BOOTSTRAP_STAGE_ORDER.index(stage_value)
        passed = list(BOOTSTRAP_STAGE_ORDER[:failed_index])
        not_checked = list(BOOTSTRAP_STAGE_ORDER[failed_index + 1 :])
        return (passed, [stage_value], not_checked)

    if "validate_sdk_contract" in startup_steps:
        return (list(BOOTSTRAP_STAGE_ORDER), [], [])

    return ([], [], list(BOOTSTRAP_STAGE_ORDER))
