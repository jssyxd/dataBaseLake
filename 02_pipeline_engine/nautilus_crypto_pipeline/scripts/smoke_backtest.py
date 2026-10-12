import sys
import time
from decimal import Decimal
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.currencies import BTC, USDT
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.trading.strategy import Strategy

# 1. Load data from catalog
catalog = ParquetDataCatalog("/tmp/nautilus_test_catalog")
bar_type = BarType.from_str("BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL")
bars = catalog.bars(bar_types=[bar_type])
print(f"Loaded {len(bars)} bars for backtest.")

# 2. Define Instrument
instrument = CryptoPerpetual(
    instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
    raw_symbol=Symbol("BTCUSDT"),
    base_currency=BTC,
    quote_currency=USDT,
    settlement_currency=USDT,
    price_precision=2,
    size_precision=5,
    price_increment=Decimal("0.01"),
    size_increment=Decimal("0.00001"),
    lot_size=Decimal("0.00001"),
    max_quantity=Decimal("1000.0"),
    min_quantity=Decimal("0.00001"),
    max_notional=Decimal("100000000.0"),
    min_notional=Decimal("5.0"),
    margin_init=Decimal("0.01"),
    margin_maint=Decimal("0.005"),
    maker_fee=Decimal("0.0002"),
    taker_fee=Decimal("0.0005"),
    ts_event=bars[0].ts_event,
    ts_init=bars[0].ts_init,
)

# 3. Simple Strategy
class SmokeTestStrategy(Strategy):
    def __init__(self, config=None):
        super().__init__(config)
        self.bar_count = 0

    def on_bar(self, bar):
        self.bar_count += 1
        if self.bar_count % 1000 == 0:
            print(f"Strategy processed bar #{self.bar_count}: {bar.close}", flush=True)

# 4. Setup Engine
engine_config = BacktestEngineConfig(
    trader_id="SMOKE-TEST-001",
    logging=LoggingConfig(log_level="ERROR"),
)
engine = BacktestEngine(config=engine_config)

# Add venue
venue = Venue("BINANCE")
engine.add_venue(
    venue=venue,
    oms_type=OmsType.NETTING,
    account_type=AccountType.MARGIN,
    base_currency=USDT,
    starting_balances=[Decimal("100000") * USDT],
)

# Add instrument
engine.add_instrument(instrument)

# Add data
engine.add_data(bars)

# Add strategy
strategy = SmokeTestStrategy()
engine.add_strategy(strategy)

# Run backtest
print("Starting backtest replay...")
t0 = time.perf_counter()
engine.run()
t1 = time.perf_counter()
duration = t1 - t0
rate = len(bars) / duration if duration > 0 else 0

print(f"Backtest completed successfully in {duration:.4f}s!")
print(f"Total bars replayed: {strategy.bar_count}")
print(f"Throughput: {rate:,.1f} bars/sec")

engine.dispose()
print("Engine disposed cleanly.")
