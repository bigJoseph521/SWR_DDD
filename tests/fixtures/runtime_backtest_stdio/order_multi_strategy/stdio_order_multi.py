from alphovex_sdk.enums.order import OrderSide, OrderType
from alphovex_sdk.models import Bar, OrderIntent
from alphovex_sdk.strategy import Strategy as _SdkStrategy

__strategy_sdk_version__ = "1.0"


class OrderMultiStrategy(_SdkStrategy):
    def on_init(self) -> None:
        return None

    def on_bar(self, bar: Bar) -> None:
        instrument = str(bar.instrument_id or bar.symbol or "AAPL")
        price = float(bar.close)
        for qty in (1.0, 2.0):
            self.buy(
                OrderIntent(
                    instrument_id=instrument,
                    side=OrderSide.BUY,
                    quantity=qty,
                    price=price,
                    order_type=OrderType.MARKET,
                )
            )
