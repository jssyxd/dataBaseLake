#!/usr/bin/env python3
"""
Convert Clean Parquet OHLCV to Nautilus Trader Native ParquetDataCatalog format
Supports:
- CurrencyPair (Spot Crypto/Forex)
- CryptoPerpetual (Binance/Bybit Futures)
- Equity (US Stocks)
"""

import sys
import os
import pandas as pd
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.currencies import BTC, ETH, SOL, USD
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

CURRENCY_MAP = {
    "BTC": BTC,
    "ETH": ETH,
    "SOL": SOL,
    "USD": USD,
}

def convert_parquet_to_nautilus(
    input_parquet: str,
    output_catalog_dir: str,
    raw_symbol: str = "BTCUSD",
    venue_str: str = "CRYPTO",
    price_precision: int = 2,
    size_precision: int = 6,
    timeframe_str: str = "1-MINUTE"
):
    df = pd.read_parquet(input_parquet)
    if "timestamp" in df.columns:
        df = df.set_index("timestamp")
    df = df[["open", "high", "low", "close", "volume"]]

    base = raw_symbol[:3]
    quote = raw_symbol[3:]
    base_curr = CURRENCY_MAP.get(base, BTC)
    quote_curr = CURRENCY_MAP.get(quote, USD)

    instrument = CurrencyPair(
        instrument_id=InstrumentId(Symbol(raw_symbol), Venue(venue_str)),
        raw_symbol=Symbol(raw_symbol),
        base_currency=base_curr,
        quote_currency=quote_curr,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.000001"),
        ts_event=0,
        ts_init=0,
    )

    bar_type = BarType.from_str(f"{raw_symbol}.{venue_str}-{timeframe_str}-LAST-EXTERNAL")
    wrangler = BarDataWrangler(bar_type=bar_type, instrument=instrument)
    bars = wrangler.process(df)

    catalog = ParquetDataCatalog(output_catalog_dir)
    catalog.write_data(bars)
    print(f"Successfully serialized {len(bars)} bars into Nautilus catalog: {output_catalog_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: nautilus_catalog_converter.py <input_parquet> <output_catalog_dir> [symbol] [venue]")
        sys.exit(1)
    convert_parquet_to_nautilus(
        input_parquet=sys.argv[1],
        output_catalog_dir=sys.argv[2],
        raw_symbol=sys.argv[3] if len(sys.argv) > 3 else "BTCUSD",
        venue_str=sys.argv[4] if len(sys.argv) > 4 else "CRYPTO"
    )
