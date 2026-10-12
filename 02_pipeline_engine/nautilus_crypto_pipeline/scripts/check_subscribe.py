from nautilus_trader.trading.strategy import Strategy

class MyStrategy(Strategy):
    def on_bar(self, bar):
        print("Got bar:", bar)

strat = MyStrategy()
for m in dir(strat):
    if "subscribe" in m:
        print("Subscribe method:", m)
