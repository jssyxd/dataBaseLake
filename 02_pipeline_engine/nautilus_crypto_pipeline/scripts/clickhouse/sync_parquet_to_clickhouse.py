"""
ClickHouse Parquet Ingestion Pipeline
Loads NautilusTrader standardized Parquet data into ClickHouse TSDB
Zero-copy batch insert via clickhouse-connect or clickhouse-driver
"""

import os
import sys
import glob
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

def generate_clickhouse_import_commands(catalog_dir: Path):
    """
    Generates native ClickHouse CLI commands to directly stream Parquet files into ClickHouse:
    cat file.parquet | clickhouse-client --query="INSERT INTO market_data.bars_1m FORMAT Parquet"
    """
    parquet_files = list((catalog_dir / "data" / "bar").rglob("*.parquet"))
    print(f"-- Discovered {len(parquet_files)} parquet files ready for ClickHouse import")
    for f in parquet_files[:5]:
        posix_p = f.as_posix()
        print(f"# Example command for {f.name}:")
        print(f"clickhouse-client --query=\"INSERT INTO market_data.bars_multi_tf FORMAT Parquet\" < \"{posix_p}\"")

if __name__ == "__main__":
    base = Path(r"//fnos2/iflow/数据库/catalog")
    generate_clickhouse_import_commands(base)
