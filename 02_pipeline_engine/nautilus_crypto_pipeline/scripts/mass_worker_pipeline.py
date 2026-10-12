"""
Mass Ingestion Pipeline for Binance & Macro Datasets
Downloads official Binance Vision S3 archives (1m Klines, aggTrades, fundingRate)
and solidifies into Nautilus ParquetDataCatalog format on NAS disk.
Supports direct ZIP stream extraction to Parquet without bloating disk with raw uncompressed CSVs.
"""

import os
import sys
import argparse
import urllib.request
import zipfile
import io
import time
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),
    ("ts_event", pa.int64()),   # nanoseconds UTC
    ("ts_init", pa.int64()),    # nanoseconds UTC
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),
    ("trades_count", pa.int64())
])

def to_nanoseconds(series: pd.Series) -> pd.Series:
    sample = series.iloc[0]
    num_digits = len(str(int(sample)))
    if num_digits == 10:       # seconds
        return series.astype('int64') * 1_000_000_000
    elif num_digits == 13:     # milliseconds
        return series.astype('int64') * 1_000_000
    elif num_digits == 16:     # microseconds
        return series.astype('int64') * 1_000
    elif num_digits == 19:     # nanoseconds
        return series.astype('int64')
    else:
        raise ValueError(f"Unknown timestamp digit length: {num_digits} for sample {sample}")

def download_and_solidify_kline(symbol: str, year: int, month: int, catalog_dir: Path, archive_dir: Path):
    month_str = f"{month:02d}"
    period = f"{year}-{month_str}"
    url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/1m/{symbol}-1m-{period}.zip"
    
    zip_target_dir = archive_dir / symbol / "klines"
    zip_target_dir.mkdir(parents=True, exist_ok=True)
    zip_file_path = zip_target_dir / f"{symbol}-1m-{period}.zip"

    # 1. Download ZIP
    if not zip_file_path.exists() or zip_file_path.stat().st_size < 1000:
        print(f"[{symbol}] Downloading {period} from {url}...")
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                with open(zip_file_path, "wb") as f:
                    f.write(data)
            print(f"[{symbol}] Saved {zip_file_path.name} ({round(len(data)/1024/1024, 2)} MB)")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"[{symbol}] {period} not found on Binance Vision S3 (HTTP 404), skipping.")
                return
            else:
                print(f"[{symbol}] HTTP error downloading {period}: {e}")
                return
        except Exception as e:
            print(f"[{symbol}] Failed downloading {period}: {e}")
            return

    # 2. Extract and Parse
    try:
        with zipfile.ZipFile(zip_file_path) as zf:
            csv_name = [name for name in zf.namelist() if name.endswith(".csv")][0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, header=None)
                if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
                    df = df.iloc[1:]

        sub = pd.DataFrame()
        bar_type_1m = f"{symbol}-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"
        sub['ts_event'] = to_nanoseconds(df[0])
        sub['ts_init'] = to_nanoseconds(df[6])
        sub['bar_type'] = bar_type_1m
        sub['open'] = df[1].astype('float64')
        sub['high'] = df[2].astype('float64')
        sub['low'] = df[3].astype('float64')
        sub['close'] = df[4].astype('float64')
        sub['volume'] = df[5].astype('float64')
        sub['quote_volume'] = df[7].astype('float64')
        sub['trades_count'] = df[8].astype('int64')

        # 3. Write 1m Parquet Partition
        year_dir = catalog_dir / "data" / "bar" / bar_type_1m / f"{symbol}-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_parquet = year_dir / f"{year}_{month_str}_1m.parquet"

        table = pa.Table.from_pandas(sub, schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(table, out_parquet, compression='zstd')
        print(f"[{symbol}] Solidified {len(sub)} 1m bars -> {out_parquet.name}")

    except Exception as e:
        print(f"[{symbol}] Error processing {period}: {e}")

def download_and_save_aggtrades(symbol: str, year: int, month: int, archive_dir: Path):
    month_str = f"{month:02d}"
    period = f"{year}-{month_str}"
    url = f"https://data.binance.vision/data/futures/um/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{period}.zip"
    
    target_dir = archive_dir / symbol / "aggTrades"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{symbol}-aggTrades-{period}.zip"

    if target_file.exists() and target_file.stat().st_size > 10000:
        print(f"[{symbol}] aggTrades {period} already downloaded ({round(target_file.stat().st_size/1024/1024, 2)} MB)")
        return

    print(f"[{symbol}] Downloading aggTrades {period} (Heavy tick data)...")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
            with open(target_file, "wb") as f:
                f.write(data)
        print(f"[{symbol}] Saved aggTrades {period} -> {target_file.name} ({round(len(data)/1024/1024, 2)} MB)")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"[{symbol}] aggTrades {period} not found (HTTP 404), skipping.")
        else:
            print(f"[{symbol}] HTTP error downloading aggTrades {period}: {e}")
    except Exception as e:
        print(f"[{symbol}] Error downloading aggTrades {period}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Mass Data Worker Pipeline")
    parser.add_argument("--symbol", type=str, required=True, help="e.g. BTCUSDT, ETHUSDT, SOLUSDT, UNIUSDT")
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--include-aggtrades", action="store_true", help="Download aggTrades as well (huge size)")
    args = parser.parse_args()

    base_dir = Path(r"\\fnos2\iflow\数据库")
    catalog_dir = base_dir / "catalog"
    archive_dir = base_dir / "raw_archive"

    print(f"=== Starting Mass Ingestion for {args.symbol} ({args.start_year} to {args.end_year}) ===")
    for year in range(args.start_year, args.end_year + 1):
        for month in range(1, 13):
            download_and_solidify_kline(args.symbol, year, month, catalog_dir, archive_dir)
            if args.include_aggtrades:
                download_and_save_aggtrades(args.symbol, year, month, archive_dir)

    print(f"=== Completed Mass Ingestion for {args.symbol}! ===")

if __name__ == "__main__":
    main()
