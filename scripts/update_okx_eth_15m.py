#!/usr/bin/env python3
"""
OKX ETH-USDT-SWAP 15m Incremental Pipeline
=========================================
Fetches the latest 15m candles for ETH-USDT-SWAP from OKX Public API,
merges with the raw lake parquet, normalizes to right-close governance format,
validates the 5 institutional mathematical invariants, and synchronizes to
both local and /mnt/fnos/iflow/database.
"""

import sys
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from pathlib import Path
import shutil

BASE_DIR = Path(__file__).resolve().parents[1]
REMOTE_BASE_DIR = Path("/mnt/fnos/iflow/database")

RAW_PATH = BASE_DIR / "01_raw_lake" / "okx" / "futures_swap" / "ETH-USDT-SWAP" / "15m" / "ETH-USDT-SWAP_15m_raw.parquet"
GOV_PATH = BASE_DIR / "03_governed_lake" / "v2_parquet_lake" / "crypto" / "futures_swap" / "ETH-USDT-SWAP" / "15m" / "ETH-USDT-SWAP_15m_governed.parquet"

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
INST_ID = "ETH-USDT-SWAP"
BAR = "15m"
BAR_MS = 15 * 60 * 1000

def fetch_okx_candles(limit=300):
    print(f"Fetching latest {limit} candles for {INST_ID} {BAR} from OKX API...")
    resp = requests.get(OKX_CANDLES_URL, params={"instId": INST_ID, "bar": BAR, "limit": str(limit)}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "0":
        raise RuntimeError(f"OKX API error: {data.get('msg')}")
    return data.get("data", [])

def main():
    print("=" * 80)
    print(f"Starting OKX {INST_ID} 15m Pipeline")
    print("=" * 80)

    # 1. Load existing raw data
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Existing raw file not found at {RAW_PATH}")

    df_raw = pd.read_parquet(RAW_PATH)
    print(f"Loaded existing raw data: {len(df_raw)} rows, range: "
          f"{datetime.fromtimestamp(df_raw['ts_left_open'].min()/1000, tz=timezone.utc)} -> "
          f"{datetime.fromtimestamp(df_raw['ts_left_open'].max()/1000, tz=timezone.utc)}")

    # 2. Fetch latest candles
    new_candles = fetch_okx_candles(limit=300)
    print(f"Fetched {len(new_candles)} candles from OKX.")

    df_new = pd.DataFrame(new_candles, columns=[
        "ts_left_open", "open", "high", "low", "close", "volume", "volume_ccy", "volume_ccy_quote", "confirm"
    ])
    df_new["ts_left_open"] = df_new["ts_left_open"].astype("int64")
    for col in ["open", "high", "low", "close", "volume", "volume_ccy", "volume_ccy_quote"]:
        df_new[col] = df_new[col].astype("float64")
    df_new["confirm"] = df_new["confirm"].astype("str")

    # 3. Merge: replace older matching timestamps with newer confirmed bars, append new
    combined_dict = {row["ts_left_open"]: row.to_dict() for _, row in df_raw.iterrows()}
    new_bars_count = 0
    updated_bars_count = 0

    for _, row in df_new.iterrows():
        ts = row["ts_left_open"]
        if ts in combined_dict:
            # If confirm changed or newer data, update
            combined_dict[ts] = row.to_dict()
            updated_bars_count += 1
        else:
            combined_dict[ts] = row.to_dict()
            new_bars_count += 1

    df_updated_raw = pd.DataFrame(list(combined_dict.values()))
    df_updated_raw = df_updated_raw.sort_values("ts_left_open").reset_index(drop=True)

    print(f"Merge summary: updated {updated_bars_count} existing bars, added {new_bars_count} new bars.")
    print(f"Total raw rows: {len(df_updated_raw)}, range: "
          f"{datetime.fromtimestamp(df_updated_raw['ts_left_open'].min()/1000, tz=timezone.utc)} -> "
          f"{datetime.fromtimestamp(df_updated_raw['ts_left_open'].max()/1000, tz=timezone.utc)}")

    # Continuity check
    diffs = df_updated_raw["ts_left_open"].diff().dropna()
    gap_count = (diffs != BAR_MS).sum()
    if gap_count > 0:
        print(f"[WARN] Found {gap_count} gaps in 15m time continuity.")
    else:
        print("[SUCCESS] 100% strictly continuous 15m time grid verified (zero gaps, zero duplicates).")

    # 4. Save updated raw parquet
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_updated_raw.to_parquet(RAW_PATH, index=False, engine="pyarrow", compression="snappy")
    print(f"[SUCCESS] Saved updated raw parquet to {RAW_PATH} ({RAW_PATH.stat().st_size / 1024:.1f} KB)")

    # 5. Build governed dataset
    # Rule: only include confirmed closed bars (confirm == '1')
    df_confirmed = df_updated_raw[df_updated_raw["confirm"] == "1"].copy()
    
    gov_rows = {
        "ts_event": df_confirmed["ts_left_open"] + BAR_MS,
        "ts_init": df_confirmed["ts_left_open"],
        "datetime_open": pd.to_datetime(df_confirmed["ts_left_open"], unit="ms", utc=True),
        "datetime_close": pd.to_datetime(df_confirmed["ts_left_open"] + BAR_MS, unit="ms", utc=True),
        "open": df_confirmed["open"],
        "high": df_confirmed["high"],
        "low": df_confirmed["low"],
        "close": df_confirmed["close"],
        "volume": df_confirmed["volume"],
        "quote_volume": df_confirmed["volume_ccy_quote"]
    }
    df_gov = pd.DataFrame(gov_rows)
    df_gov = df_gov.sort_values("ts_event").reset_index(drop=True)

    # 6. Mathematical Invariants Verification
    issues = []
    if not df_gov["ts_event"].is_monotonic_increasing:
        issues.append("ts_event is not monotonic increasing")
    if df_gov["ts_event"].duplicated().any():
        issues.append(f"Found {df_gov['ts_event'].duplicated().sum()} duplicate ts_event")
    
    h_viol = (df_gov["high"] < df_gov["open"]) | (df_gov["high"] < df_gov["close"])
    l_viol = (df_gov["low"] > df_gov["open"]) | (df_gov["low"] > df_gov["close"])
    hl_viol = df_gov["low"] > df_gov["high"]
    zero_viol = (df_gov["open"] <= 0) | (df_gov["high"] <= 0) | (df_gov["low"] <= 0) | (df_gov["close"] <= 0)
    vol_viol = (df_gov["volume"] < 0) | (df_gov["quote_volume"] < 0)

    if h_viol.any():
        issues.append(f"High price invariant violated in {h_viol.sum()} rows")
    if l_viol.any():
        issues.append(f"Low price invariant violated in {l_viol.sum()} rows")
    if hl_viol.any():
        issues.append(f"Low > High violation in {hl_viol.sum()} rows")
    if zero_viol.any():
        issues.append(f"Non-positive price found in {zero_viol.sum()} rows")
    if vol_viol.any():
        issues.append(f"Negative volume found in {vol_viol.sum()} rows")

    if issues:
        raise ValueError(f"Mathematical invariants failed: {issues}")
    print("[SUCCESS] All 5 institutional mathematical invariants PASSED (100% compliant).")

    # Save governed parquet
    GOV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_gov.to_parquet(GOV_PATH, index=False, engine="pyarrow", compression="snappy")
    print(f"[SUCCESS] Saved governed parquet to {GOV_PATH} ({len(df_gov)} rows, {GOV_PATH.stat().st_size / 1024:.1f} KB)")
    print(f"Governed range: {df_gov['datetime_open'].min()} -> {df_gov['datetime_close'].max()} (latest closed bar)")

    # 7. Synchronize to /mnt/fnos/iflow/database
    if REMOTE_BASE_DIR.exists():
        remote_raw = REMOTE_BASE_DIR / "01_raw_lake" / "okx" / "futures_swap" / "ETH-USDT-SWAP" / "15m" / "ETH-USDT-SWAP_15m_raw.parquet"
        remote_gov = REMOTE_BASE_DIR / "03_governed_lake" / "v2_parquet_lake" / "crypto" / "futures_swap" / "ETH-USDT-SWAP" / "15m" / "ETH-USDT-SWAP_15m_governed.parquet"

        remote_raw.parent.mkdir(parents=True, exist_ok=True)
        remote_gov.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(RAW_PATH, remote_raw)
        shutil.copy2(GOV_PATH, remote_gov)
        print(f"[SUCCESS] Synchronized both files to {REMOTE_BASE_DIR}")
        print(f"  Remote Raw: {remote_raw} ({remote_raw.stat().st_size / 1024:.1f} KB)")
        print(f"  Remote Governed: {remote_gov} ({remote_gov.stat().st_size / 1024:.1f} KB)")
    else:
        print(f"[WARN] Remote directory {REMOTE_BASE_DIR} not found, skipped remote sync.")

if __name__ == "__main__":
    main()
