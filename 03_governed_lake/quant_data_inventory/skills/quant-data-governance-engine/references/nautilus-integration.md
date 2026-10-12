# Nautilus Trader Parquet Catalog Integration Guide

## 1. Directory Structure
Nautilus Trader `ParquetDataCatalog` uses the following path convention:
```text
<catalog_root>/
└── data/
    └── bar/
        └── <bar_type>/
            └── <chunk_start_ts>_<chunk_end_ts>.parquet
```
Where `bar_type` follows:
`<symbol>.<exchange>-<spec>-<aggregation>-<price_type>`
Examples:
- `BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL`
- `ETHUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL`

## 2. Ingestion Code Pattern
```python
import pandas as pd
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.currencies import BTC, USD
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

# 1. Define instrument specification
instrument = CurrencyPair(
    instrument_id=InstrumentId(Symbol("BTCUSD"), Venue("CRYPTO")),
    raw_symbol=Symbol("BTCUSD"),
    base_currency=BTC,
    quote_currency=USD,
    price_precision=2,
    size_precision=6,
    price_increment=Price.from_str("0.01"),
    size_increment=Quantity.from_str("0.000001"),
    ts_event=0,
    ts_init=0,
)

# 2. Process clean DataFrame with BarDataWrangler
bar_type = BarType.from_str("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL")
df = pd.read_parquet("sample_nautilus_bars.parquet").set_index("timestamp")
wrangler = BarDataWrangler(bar_type=bar_type, instrument=instrument)
bars = wrangler.process(df[["open", "high", "low", "close", "volume"]])

# 3. Write to Nautilus Parquet Catalog
catalog = ParquetDataCatalog("/path/to/catalog")
catalog.write_data(bars)

# 4. Direct Backtest Query
loaded_bars = catalog.bars(bar_types=[str(bar_type)])
print(f"Loaded {len(loaded_bars)} bars ready for backtesting.")
```
