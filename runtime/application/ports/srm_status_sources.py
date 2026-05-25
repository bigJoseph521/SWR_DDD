from __future__ import annotations

from typing import Final, Literal

SrmStatusSource = Literal["HEARTBEAT", "UPDATE"]

SRM_STATUS_SOURCE_HEARTBEAT: Final[SrmStatusSource] = "HEARTBEAT"
SRM_STATUS_SOURCE_UPDATE: Final[SrmStatusSource] = "UPDATE"
