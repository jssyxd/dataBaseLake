import time
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

catalog_path = Path("/mnt/fnos2_iflow/数据库/catalog")
sample_file = catalog_path / "data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet"

print("Original file:", sample_file)
table = pq.read_table(str(sample_file))
print("Original schema metadata:", table.schema.metadata)

# Create schema with Nautilus metadata
new_metadata = {
    b"bar_type": b"BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL",
    b"instrument_id": b"BTCUSDT-PERP.BINANCE",
    b"price_precision": b"2",
    b"size_precision": b"5",
}
if table.schema.metadata:
    merged_metadata = dict(table.schema.metadata)
    merged_metadata.update(new_metadata)
else:
    merged_metadata = new_metadata

new_table = table.replace_schema_metadata(merged_metadata)
test_out = Path("/tmp/test_bar_with_meta.parquet")
pq.write_table(new_table, str(test_out), compression='zstd')
print("Written test table with metadata:", test_out)

# Now test catalog.query on this test file
cat = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")
t0 = time.perf_counter()
bars = cat.query(Bar, files=[str(test_out)])
t1 = time.perf_counter()

print(f"Success! Loaded {len(bars)} bars via catalog.query(Bar) in {t1 - t0:.4f}s")
if len(bars) > 0:
    print("First bar:", bars[0])
    print("Last bar: ", bars[-1])
