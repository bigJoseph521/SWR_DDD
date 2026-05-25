from alphovex_sdk.models import Bar
from alphovex_sdk.strategy import Strategy as _SdkStrategy

__strategy_sdk_version__ = "1.0"


class BarRaisesStrategy(_SdkStrategy):
    def on_init(self) -> None:
        return None

    def on_bar(self, bar: Bar) -> None:
        _ = bar
        raise RuntimeError("on_bar failed on purpose")
