#!/usr/bin/env python3
"""
CMC20 (CoinMarketCap 20 Index) Historical Data Downloader & Ingestion Script
===========================================================================
Part of dataBaseLake 01_raw_lake scripts.
Downloads authoritative 15m historical CMC20 index series with full constituent breakdown
(weights, units, USD prices for Top 20 crypto assets) via CMC Keyless Public API.

Usage:
    python download_cmc20_index.py --start 2024-01-01 --delay 1.5
"""

import os
import sys
import time
import json
import argparse
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_TARGET_DIR = BASE_DIR / "01_raw_lake" / "cmc_indices" / "CMC20"
BASE_URL = "https://pro-api.coinmarketcap.com/public-api/v3/index/cmc20-historical"
INTERVAL = "15m"
BATCH = 10
STEP_MIN = BATCH * 15


def fetch_window(ts_start: datetime, ts_end: datetime, delay: float = 1.5, max_retries: int = 15) -> list:
    params = urllib.parse.urlencode({
        "interval": INTERVAL,
        "time_start": ts_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time_end": ts_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": BATCH,
    })
    url = f"{BASE_URL}?{params}"
    backoff = delay
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8")).get("data", [])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(backoff * (1.8 ** attempt), 240)
                print(f"  [429 Throttled] Backing off: waiting {wait:.1f}s ...", flush=True)
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            wait = min(backoff * (1.8 ** attempt), 120)
            print(f"  [Network exception: {e}] Retrying in {wait:.1f}s ...", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Max retries exceeded for URL: {url}")


def flatten_row(row: dict) -> dict:
    out = {"timestamp": row["update_time"], "value": row["value"]}
    for c in row.get("constituents", []):
        s = c["symbol"].upper()
        out[f"{s}_weight"] = c.get("weight")
        out[f"{s}_price_usd"] = c.get("priceUsd")
        out[f"{s}_units"] = c.get("units")
    return out


def main():
    parser = argparse.ArgumentParser(description="CMC20 Historical Index Downloader")
    parser.add_argument("--start", default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD), default=now")
    parser.add_argument("--delay", type=float, default=1.5, help="Request delay seconds")
    parser.add_argument("--output-dir", default=str(DEFAULT_TARGET_DIR), help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts_start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    ts_end = (datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
              if args.end else datetime.now(timezone.utc))

    import pandas as pd
    parquet_path = out_dir / "cmc20_15m_raw.parquet"
    csv_path = out_dir / "cmc20_15m_raw.csv"

    cursor = ts_start
    existing_df = None
    existing_ts = set()

    if parquet_path.exists():
        existing_df = pd.read_parquet(parquet_path)
        existing_df["timestamp"] = pd.to_datetime(existing_df["timestamp"], utc=True)
        existing_ts = set(existing_df["timestamp"].astype(str))
        cursor = existing_df["timestamp"].max().to_pydatetime() + timedelta(minutes=15)
        print(f"Resuming from existing parquet: {cursor.isoformat()} ({len(existing_df):,} rows)")

    total_est = int((ts_end - cursor).total_seconds() / 60 / STEP_MIN) + 1
    print(f"Downloading CMC20 {INTERVAL} from {cursor.date()} to {ts_end.date()} (~{total_est} batches)")

    new_rows = []
    req_count = 0
    while cursor <= ts_end:
        window_end = min(cursor + timedelta(minutes=STEP_MIN - 1), ts_end)
        rows = fetch_window(cursor, window_end, delay=args.delay)
        if rows:
            for r in rows:
                flat = flatten_row(r)
                if flat["timestamp"] not in existing_ts:
                    new_rows.append(flat)
                    existing_ts.add(flat["timestamp"])
            last_rec = rows[-1]["update_time"]
            cursor = datetime.strptime(last_rec, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc) + timedelta(minutes=15)
        else:
            cursor += timedelta(minutes=STEP_MIN)
        req_count += 1
        time.sleep(args.delay)

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], utc=True)
        df_all = pd.concat([existing_df, df_new], ignore_index=True) if existing_df is not None else df_new
        df_all = df_all.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
        df_all.to_parquet(parquet_path, index=False, engine="pyarrow", compression="zstd")
        df_all.to_csv(csv_path, index=False)
        print(f"[SUCCESS] Saved {len(df_all):,} rows to {parquet_path} and {csv_path}")
    else:
        print("[INFO] No new rows to append; dataset up-to-date.")


if __name__ == "__main__":
    main()
