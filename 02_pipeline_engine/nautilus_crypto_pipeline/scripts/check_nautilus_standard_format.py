import tempfile
import shutil
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

# Check ArrowSerializer._serialize_rust or catalog.write_data
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2
import pandas as pd

# Create dummy sample df
df = pd.DataFrame({
    'timestamp': [pd.Timestamp('2026-01-01 00:00:00', tz='UTC')],
    'open': [100.0],
    'high': [105.0],
    'low': [99.0],
    'close': [102.0],
    'volume': [10.0]
})

wrangler = BarDataWranglerV2(
    bar_type="BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL",
    price_precision=2,
    size_precision=5
)
bars = wrangler.from_pandas(df)
print("Created sample bar:", bars[0])

tmp_dir = Path("/tmp/test_nautilus_cat")
if tmp_dir.exists():
    shutil.rmtree(tmp_dir)
tmp_dir.mkdir(parents=True, exist_ok=True)

cat = ParquetDataCatalog(str(tmp_dir))
cat.write_data(bars)
print("Wrote bars to catalog!")

# Find written parquet file
files = list(tmp_dir.glob("**/*.parquet"))
print(f"Written files ({len(files)}):", files)
if files:
    schema = pq.read_schema(str(files[0]))
    print("Schema fields:")
    for f in schema:
        print(" -", f.name, f.type)
    print("Schema metadata:")
    for k, v in (schema.metadata or {}).items():
        print(" -", k, v)

# Now test reading back via cat.bars()
read_bars = cat.bars()
print(f"Read back {len(read_bars)} bars via cat.bars():", read_bars[0])
