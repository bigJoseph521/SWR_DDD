from alphovex_sdk.strategy import Strategy as _SdkStrategy

__strategy_sdk_version__ = "1.0"


class MinimalStrategy(_SdkStrategy):
    def on_init(self) -> None:
        return None
