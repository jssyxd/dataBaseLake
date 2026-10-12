from nautilus_trader.persistence.catalog import ParquetDataCatalog
cat = ParquetDataCatalog("/tmp/nautilus_test_catalog")
print("Data types:", cat.list_data_types())
print("Default bars():", len(cat.bars()))
import inspect
print(inspect.signature(cat.bars))
