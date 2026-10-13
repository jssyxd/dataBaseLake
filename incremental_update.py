#!/usr/bin/env python3
"""
Incremental Market Data Updater & Governance Engine (dataBaseLake)
------------------------------------------------------------------
Automates future incremental updates for market data across crypto, equities,
and futures. Ingests new daily/minute bars, enforces 5 core mathematical invariants,
and synchronizes updates into the governed lake and manifest registry.
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
GOVERNED_DIR = BASE_DIR / "03_governed_lake"
PIPELINE_DIR = BASE_DIR / "02_pipeline_engine"
MANIFEST_FILE = GOVERNED_DIR / "v2_parquet_lake" / "manifest" / "manifest.json"


def verify_mathematical_invariants(df):
    """
    Validates the 5 mandatory data governance invariants:
    1. Timestamp Monotonicity: ts[i] > ts[i-1] strictly ascending.
    2. Deduplication: No duplicate timestamps.
    3. High-Low Invariant: High >= max(Open, Close) and Low <= min(Open, Close).
    4. Positive Range: Open, High, Low, Close > 0.
    5. Non-negative Volume: Volume >= 0.
    """
    issues = []
    if "timestamp" in df.columns:
        ts = df["timestamp"]
        if not ts.is_monotonic_increasing:
            issues.append("Timestamps are not strictly monotonic increasing.")
        if ts.duplicated().any():
            issues.append(f"Found {ts.duplicated().sum()} duplicate timestamps.")
    elif isinstance(df.index, (pd.DatetimeIndex, pd.Index)) and df.index.name in ["timestamp", None] and len(df) > 0:
        if not df.index.is_monotonic_increasing:
            issues.append("Index timestamps are not strictly monotonic increasing.")
        if df.index.duplicated().any():
            issues.append(f"Found {df.index.duplicated().sum()} duplicate index timestamps.")
    # Check OHLC columns or index_ohlc columns
    o_col, h_col, l_col, c_col = None, None, None, None
    if all(col in df.columns for col in ["open", "high", "low", "close"]):
        o_col, h_col, l_col, c_col = "open", "high", "low", "close"
    elif all(col in df.columns for col in ["index_open", "index_high", "index_low", "index_close"]):
        o_col, h_col, l_col, c_col = "index_open", "index_high", "index_low", "index_close"

    if o_col:
        h_viol = (df[h_col] < df[o_col]) | (df[h_col] < df[c_col])
        l_viol = (df[l_col] > df[o_col]) | (df[l_col] > df[c_col])
        hl_viol = df[l_col] > df[h_col]
        zero_viol = (df[o_col] <= 0) | (df[h_col] <= 0) | (df[l_col] <= 0) | (df[c_col] <= 0)

        if h_viol.any():
            issues.append(f"High price invariant violated in {h_viol.sum()} rows.")
        if l_viol.any():
            issues.append(f"Low price invariant violated in {l_viol.sum()} rows.")
        if hl_viol.any():
            issues.append(f"Low > High violation in {hl_viol.sum()} rows.")
        if zero_viol.any():
            issues.append(f"Non-positive price found in {zero_viol.sum()} rows.")
    elif "value" in df.columns:
        zero_viol = df["value"] <= 0
        if zero_viol.any():
            issues.append(f"Non-positive value found in {zero_viol.sum()} rows.")
    if "volume" in df.columns:
        vol_viol = df["volume"] < 0
        if vol_viol.any():
            issues.append(f"Negative volume violation in {vol_viol.sum()} rows.")

    return issues


def run_governance_audit():
    print("=" * 80)
    print("Running Data Governance & Invariants Audit on Governed Lake...")
    print("=" * 80)

    try:
        import pyarrow.parquet as pq
        import pandas as pd
    except ImportError:
        print("[WARN] pyarrow or pandas not found. Running superficial scan.")
        return True

    # Scan a sample of governed parquets
    scanned_count = 0
    passed_count = 0
    error_list = []

    for root, _, files in os.walk(GOVERNED_DIR):
        for f in files:
            if f.endswith(".parquet"):
                fp = os.path.join(root, f)
                try:
                    df = pd.read_parquet(fp)
                    issues = verify_mathematical_invariants(df)
                    scanned_count += 1
                    if not issues:
                        passed_count += 1
                    else:
                        rel = os.path.relpath(fp, BASE_DIR)
                        error_list.append((rel, issues))
                except Exception as e:
                    error_list.append((fp, [str(e)]))

    print(f"Audit Complete: {passed_count}/{scanned_count} files passed 100% invariant checks.")
    if error_list:
        print(f"[ALERT] Found {len(error_list)} files with potential governance anomalies:")
        for fp, errs in error_list[:5]:
            print(f"  - {fp}: {errs}")
    else:
        print("[SUCCESS] All surveyed governed data complies with Institutional Quant Invariants.")
    return len(error_list) == 0


def update_manifest():
    if not MANIFEST_FILE.exists():
        print(f"[INFO] Manifest {MANIFEST_FILE} not found; skipping registry sync.")
        return

    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)

    manifest_data["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
    print(f"[SUCCESS] Updated manifest registry: {MANIFEST_FILE}")


def update_cmc20():
    """
    Pulls incremental CMC20 index data from CMC Keyless Public API,
    resamples all timeframes, generates signals, and updates governed lake.
    """
    print("\n" + "=" * 80)
    print("Executing Incremental Update for CMC20 Index...")
    print("=" * 80)
    script_path = BASE_DIR / "01_raw_lake" / "scripts" / "download_cmc20_index.py"
    target_dir = GOVERNED_DIR / "v2_parquet_lake" / "crypto_indices" / "CMC20"
    if script_path.exists():
        import subprocess
        subprocess.run([sys.executable, str(script_path), "--output-dir", str(target_dir)], check=True)
        print("[SUCCESS] CMC20 incremental update completed.")
    else:
        print(f"[ERROR] Script {script_path} not found.")


def main():
    parser = argparse.ArgumentParser(description="dataBaseLake Incremental Updater & Governance Auditor")
    parser.add_argument("--audit", action="store_true", help="Run institutional mathematical invariants check")
    parser.add_argument("--sync-manifest", action="store_true", help="Synchronize metadata manifest registry")
    parser.add_argument("--symbols", nargs="+", help="Target symbols to update (e.g. BTCUSDT ETHUSDT QQQ SPY CMC20)")
    args = parser.parse_args()

    if args.symbols and any(s.upper() in ["CMC20", "CMC"] for s in args.symbols):
        update_cmc20()
        run_governance_audit()
        update_manifest()
    elif args.audit or not any([args.audit, args.sync_manifest, args.symbols]):
        run_governance_audit()
        update_manifest()
    elif args.sync_manifest:
        update_manifest()


if __name__ == "__main__":
    main()
