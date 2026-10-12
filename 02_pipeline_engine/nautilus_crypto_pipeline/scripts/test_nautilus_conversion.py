import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

# Load 1 year of our catalog parquet (which has standard float columns)
src_file = "/mnt/fnos2_iflow/数据库/catalog/data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet"
df = pd.read_parquet(src_file)
print("Read df from parquet:", df.shape)
print(df.head(2))

# Use BarDataWranglerV2 to convert DataFrame to Nautilus Bar objects
bar_type_str = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
wrangler = BarDataWranglerV2(
    bar_type=bar_type_str,
    price_precision=2,
    size_precision=5
)

# Rename ts_event to timestamp for wrangler
df_for_wrangler = df.rename(columns={'ts_event': 'timestamp'})
bars = wrangler.from_pandas(df_for_wrangler)
print(f"Wrangled {len(bars)} Nautilus Bar objects.")
print("Sample bar:", bars[0])

# Now write them using Nautilus ParquetDataCatalog.write_data
cat_dir = "/tmp/nautilus_test_catalog"
catalog = ParquetDataCatalog(cat_dir)
catalog.write_data(bars)
print("Successfully written to ParquetDataCatalog!")

# Query back
queried_bars = catalog.bars()
print(f"Successfully loaded {len(queried_bars)} bars via catalog.bars()!")
print("Queried first:", queried_bars[0])
print("Queried last: ", queried_bars[-1])
