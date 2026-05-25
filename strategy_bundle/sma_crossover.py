from alphovex_sdk import (
    Strategy,
    DataSourceEnum,
    SMA,
    Bar,
    OrderIntent,
    OrderSide,
    OrderType,
)
class SMACrossOver(Strategy):

    def on_init(self) -> None:
        self.fast = self.params.get("fast_period")
        self.slow = self.params.get("slow_period")
        self.indicator.register_indicator("sma_fast", SMA(self.fast), DataSourceEnum.BAR)
        self.indicator.register_indicator("sma_slow", SMA(self.slow), DataSourceEnum.BAR)
        self.holding = False
    
    def on_bar(self, bar: Bar) -> None:
        fast = self.indicator.get_indicator_value("sma_fast")
        slow = self.indicator.get_indicator_value("sma_slow")

        if fast is None or slow is None:
            return

        instrument = str(bar.instrument_id or bar.symbol or "")
        if not instrument:
            return
        price = float(bar.close)
        if self.holding and fast < slow:
            self.sell(
                OrderIntent(
                    instrument_id=instrument,
                    side=OrderSide.SELL,
                    quantity=1.0,
                    price=price,
                    order_type=OrderType.MARKET,
                )
            )
            self.holding = False
        elif not self.holding and fast > slow:
            self.buy(
                OrderIntent(
                    instrument_id=instrument,
                    side=OrderSide.BUY,
                    quantity=1.0,
                    price=price,
                    order_type=OrderType.MARKET,
                )
            )
            self.holding = True
