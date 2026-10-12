import inspect
from nautilus_trader.trading.strategy import Strategy
print(inspect.signature(Strategy.subscribe_bars))
print(Strategy.on_start.__doc__)
