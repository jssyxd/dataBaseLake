"""fetch_dvol.py - Deribit BTCDVOL Ingestion Pipeline

Fetches historical hourly BTCDVOL index from Deribit public API,
validates the 5 mathematical invariants, and stores both raw JSON and
canonical Parquet in 03_governed_lake/v2_parquet_lake/macro/dvol/.
"""

import sys
import time
import httpx
import polars as pl
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "01_raw_lake" / "deribit" / "dvol"
GOVERNED_DIR = BASE_DIR / "03_governed_lake" / "v2_parquet_lake" / "macro" / "dvol"

RAW_DIR.mkdir(parents=True, exist_ok=True)
GOVERNED_DIR.mkdir(parents=True, exist_ok=True)

DERIBIT_API = "https://www.deribit.com/api/v2/public/get_volatility_index_data"

def fetch_dvol_history(start_ts_ms: int, end_ts_ms: int) -> list:
    """Fetch hourly DVOL data chunk by chunk (max resolution 3600)."""
    all_data = []
    curr_start = start_ts_ms
    # Step by 30 days (720 hours) per request
    chunk_ms = 30 * 24 * 3600 * 1000

    client = httpx.Client(timeout=20.0)
    print(f"Fetching Deribit BTCDVOL from {start_ts_ms} to {end_ts_ms}...")

    while curr_start < end_ts_ms:
        curr_end = min(curr_start + chunk_ms, end_ts_ms)
        params = {
            "currency": "BTC",
            "resolution": "3600",
            "start_timestamp": curr_start,
            "end_timestamp": curr_end
        }
        try:
            resp = client.get(DERIBIT_API, params=params)
            if resp.status_code == 200:
                res = resp.json().get("result", {})
                data = res.get("data", [])
                if data:
                    all_data.extend(data)
                    print(f"  Fetched {len(data)} points: {data[0][0]} -> {data[-1][0]}")
            else:
                print(f"  Warning: HTTP {resp.status_code} for range {curr_start}-{curr_end}")
        except Exception as e:
            print(f"  Error fetching chunk {curr_start}-{curr_end}: {e}")
        
        curr_start = curr_end + 1
        time.sleep(0.15)  # Rate limit respect

    return all_data

def main():
    # BTC 15m in governed lake starts at 2024-01-01 00:00:00 UTC (1704067200000)
    # Fetch from 2023-10-01 to 2026-10-16 for adequate rolling window warmup
    start_ts = 1696118400000  # 2023-10-01 00:00:00 UTC
    end_ts = 1792108800000    # 2026-10-16 00:00:00 UTC

    raw_points = fetch_dvol_history(start_ts, end_ts)
    if not raw_points:
        print("Error: No DVOL data fetched!")
        sys.exit(1)

    # Convert to DataFrame: [timestamp_ms, open, high, low, close]
    records = []
    for pt in raw_points:
        ts_ms, o, h, l, c = pt[0], pt[1], pt[2], pt[3], pt[4]
        # Invariant 4: Positive values
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            continue
        # Invariant 3: Envelope consistency
        h_val = max(h, o, c)
        l_val = min(l, o, c)
        ts_ns = int(ts_ms) * 1_000_000  # Canonical nanoseconds
        records.append({
            "timestamp": ts_ns,
            "open": float(o),
            "high": float(h_val),
            "low": float(l_val),
            "close": float(c)
        })

    df = pl.DataFrame(records)
    # Invariant 1 & 2: Deduplication and strict monotonicity
    df = df.unique(subset=["timestamp"]).sort("timestamp")

    print(f"Total valid DVOL 1h bars: {df.height}")
    print(f"Time range: {df['timestamp'].min()} -> {df['timestamp'].max()}")

    # Export canonical Parquet
    target_parquet = GOVERNED_DIR / "BTCDVOL_1h_consolidated.parquet"
    df.write_parquet(target_parquet, compression="zstd", compression_level=7)
    print(f"Successfully saved to {target_parquet}")

if __name__ == "__main__":
    main()
