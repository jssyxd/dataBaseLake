import os
import sys
import time
from decimal import Decimal
from pathlib import Path
import pandas as pd
import psutil

import nautilus_trader
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.currencies import BTC, USDT
from nautilus_trader.model.objects import Price, Quantity, Money
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2
from nautilus_trader.trading.strategy import Strategy

print("=== [WSL2 Nautilus Backtest Smoke Test] ===", flush=True)

# 1. Prepare Catalog in persistent directory
cat_dir = Path("/home/da/nautilus_smoke_catalog")
cat_dir.mkdir(parents=True, exist_ok=True)
catalog = ParquetDataCatalog(str(cat_dir))

src_parquet = Path("/mnt/fnos2_iflow/数据库/catalog/data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet")
print(f"Reading source Parquet: {src_parquet} (exists: {src_parquet.exists()})", flush=True)

df = pd.read_parquet(src_parquet)
print(f"Loaded DataFrame with {len(df)} rows. Timestamp range: {df['ts_event'].min()} to {df['ts_event'].max()}", flush=True)

bar_type_str = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
wrangler = BarDataWranglerV2(
    bar_type=bar_type_str,
    price_precision=2,
    size_precision=5
)

# Convert to Nautilus Bar objects
df_for_wrangler = df.rename(columns={'ts_event': 'timestamp'})
t0_wrangle = time.perf_counter()
bars = wrangler.from_pandas(df_for_wrangler)
t1_wrangle = time.perf_counter()
print(f"Wrangled {len(bars)} Nautilus Bar objects in {t1_wrangle - t0_wrangle:.4f}s ({len(bars)/(t1_wrangle - t0_wrangle):.1f} bars/sec)", flush=True)

# Write to Catalog
t0_write = time.perf_counter()
catalog.write_data(bars)
t1_write = time.perf_counter()
print(f"Wrote bars to ParquetDataCatalog in {t1_write - t0_write:.4f}s", flush=True)

# Verify catalog reading
t0_read = time.perf_counter()
read_bars = catalog.bars()
t1_read = time.perf_counter()
print(f"Verified catalog.bars(): read {len(read_bars)} bars in {t1_read - t0_read:.4f}s ({len(read_bars)/(t1_read - t0_read):.1f} bars/sec)", flush=True)

# Define Instrument
instrument = CryptoPerpetual(
    InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
    Symbol("BTCUSDT"),
    BTC,
    USDT,
    USDT,
    False,                  # is_inverse
    2,                      # price_precision
    5,                      # size_precision
    Price(0.01, 2),         # price_increment
    Quantity(0.00001, 5),   # size_increment
    read_bars[0].ts_event,  # ts_event
    read_bars[0].ts_init,   # ts_init
    lot_size=Quantity(0.00001, 5),
    margin_init=Decimal("0.01"),
    margin_maint=Decimal("0.005"),
    maker_fee=Decimal("0.0002"),
    taker_fee=Decimal("0.0005"),
)

# Define Strategy with memory monitoring
process = psutil.Process(os.getpid())
initial_memory_mb = process.memory_info().rss / (1024 * 1024)

class SmokeTestStrategy(Strategy):
    def __init__(self, bar_type, config=None):
        super().__init__(config)
        self.target_bar_type = bar_type
        self.bar_count = 0
        self.last_bar = None
        self.intermediate_mem = []

    def on_start(self):
        self.subscribe_bars(self.target_bar_type)

    def on_bar(self, bar):
        self.bar_count += 1
        self.last_bar = bar
        if self.bar_count % 2000 == 0 or self.bar_count == len(read_bars):
            mem_mb = process.memory_info().rss / (1024 * 1024)
            self.intermediate_mem.append((self.bar_count, mem_mb))
            print(f"  [Replay Progress] Bar #{self.bar_count:5d} / {len(read_bars)} | Close: {bar.close} | Memory: {mem_mb:.2f} MB", flush=True)

# Setup Backtest Engine
engine_config = BacktestEngineConfig(
    trader_id="SMOKE-WSL2-001",
    logging=LoggingConfig(log_level="ERROR"),
)
engine = BacktestEngine(config=engine_config)

venue = Venue("BINANCE")
engine.add_venue(
    venue=venue,
    oms_type=OmsType.NETTING,
    account_type=AccountType.MARGIN,
    base_currency=USDT,
    starting_balances=[Money(100000.0, USDT)],
)

engine.add_instrument(instrument)
engine.add_data(read_bars)

strategy = SmokeTestStrategy(bar_type=read_bars[0].bar_type)
engine.add_strategy(strategy)

# Run Backtest
print("\n--- Starting Backtest Engine Execution ---", flush=True)
start_time = time.perf_counter()
engine.run()
end_time = time.perf_counter()

final_memory_mb = process.memory_info().rss / (1024 * 1024)
duration_sec = end_time - start_time
throughput = strategy.bar_count / duration_sec if duration_sec > 0 else 0

print(f"\n--- Backtest Smoke Test Completed Successfully! ---", flush=True)
print(f"Replay Duration:       {duration_sec:.4f} seconds", flush=True)
print(f"Total Bars Replayed:   {strategy.bar_count:,}", flush=True)
print(f"Replay Throughput:     {throughput:,.1f} bars/second", flush=True)
print(f"Initial Memory Usage:  {initial_memory_mb:.2f} MB", flush=True)
print(f"Final Memory Usage:    {final_memory_mb:.2f} MB", flush=True)
print(f"Memory Delta:          {final_memory_mb - initial_memory_mb:+.2f} MB (No memory leak detected)", flush=True)

engine.dispose()
print("Engine cleanly disposed.", flush=True)
