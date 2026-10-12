#!/usr/bin/env python3
"""
Clean, Audit, and Partition Intraday OHLCV Bars for Nautilus Trader & Quant Research.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

def clean_and_audit_ohlcv(
    input_parquet_path: str,
    output_catalog_dir: str,
    symbol: str,
    exchange: str = "CRYPTO",
    timeframe: str = "1m",
    expected_seconds: int = 60,
    write_nautilus_native: bool = True
) -> dict:
    bar_type = f"{symbol}.{exchange}-{timeframe.upper()}-LAST-EXTERNAL"
    t0 = time.time()
    
    df = pl.read_parquet(input_parquet_path)
    total_raw = len(df)

    # Standardize column naming
    col_map = {}
    for col in df.columns:
        c_low = col.lower()
        if c_low in ["ts", "time", "datetime", "date"]:
            col_map[col] = "timestamp"
        elif c_low in ["open", "high", "low", "close", "volume"]:
            col_map[col] = c_low
    df = df.rename(col_map)

    # 1. UTC Timestamp Normalization
    df = df.with_columns(
        pl.col("timestamp").dt.replace_time_zone("UTC")
    )

    # 2. Deduplication and Monotonic Sorting
    initial_len = len(df)
    df = df.sort("timestamp").unique(subset=["timestamp"], keep="last")
    dups_removed = initial_len - len(df)

    # 3. Invariant Audits
    # OHLC: H >= max(O, C), L <= min(O, C), L <= H, O,H,L,C > 0, Volume >= 0
    clean_df = df.filter(
        (pl.col("high") >= pl.col("open")) &
        (pl.col("high") >= pl.col("close")) &
        (pl.col("low") <= pl.col("open")) &
        (pl.col("low") <= pl.col("close")) &
        (pl.col("low") <= pl.col("high")) &
        (pl.col("open") > 0) & (pl.col("high") > 0) & (pl.col("low") > 0) & (pl.col("close") > 0) &
        (pl.col("volume") >= 0)
    )
    invalid_ohlc = initial_len - dups_removed - len(clean_df)

    # 4. Gap Detection (No Forward-Fill for 24/7 Crypto)
    ts_series = clean_df["timestamp"]
    diffs = ts_series.diff().dt.total_seconds()
    gap_threshold = expected_seconds * 1.5
    gaps_mask = diffs > gap_threshold
    gap_indices = [int(i) for i in gaps_mask.to_numpy().nonzero()[0]]

    gaps_report = []
    total_missing_bars = 0
    for idx in gap_indices:
        prev_ts = str(ts_series[idx - 1])
        curr_ts = str(ts_series[idx])
        gap_sec = float(diffs[idx])
        missing = int(gap_sec // expected_seconds) - 1
        total_missing_bars += missing
        if len(gaps_report) < 500:
            gaps_report.append({
                "from": prev_ts,
                "to": curr_ts,
                "gap_seconds": gap_sec,
                "missing_bars": missing
            })

    # 5. Dual Schema Enrichment
    clean_df = clean_df.with_columns([
        pl.lit(symbol).cast(pl.Utf8).alias("symbol"),
        pl.lit(timeframe).cast(pl.Utf8).alias("timeframe"),
        pl.lit(bar_type).cast(pl.Utf8).alias("bar_type"),
        (pl.col("timestamp").dt.timestamp("ns")).cast(pl.UInt64).alias("ts_event"),
        (pl.col("timestamp").dt.timestamp("ns")).cast(pl.UInt64).alias("ts_init"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("timestamp").dt.year().alias("year")
    ])

    # 6. Partitioned Write
    cat_dir = os.path.join(output_catalog_dir, "bar", bar_type)
    os.makedirs(cat_dir, exist_ok=True)
    years = clean_df["year"].unique().sort().to_list()
    total_bytes = 0

    for yr in years:
        yr_df = clean_df.filter(pl.col("year") == yr).drop("year")
        target_file = os.path.join(cat_dir, f"{yr}.parquet")
        yr_df.write_parquet(target_file, compression="zstd")
        total_bytes += os.path.getsize(target_file)

    elapsed = time.time() - t0
    return {
        "symbol": symbol,
        "bar_type": bar_type,
        "timeframe": timeframe,
        "raw_rows": total_raw,
        "clean_rows": len(clean_df),
        "start_date": str(clean_df["timestamp"].min()),
        "end_date": str(clean_df["timestamp"].max()),
        "duplicates_removed": dups_removed,
        "invalid_ohlc_removed": invalid_ohlc,
        "total_gaps": len(gap_indices),
        "missing_bars_count": total_missing_bars,
        "gaps_sample": gaps_report[:50],
        "size_bytes": total_bytes,
        "years": years,
        "elapsed_seconds": round(elapsed, 2)
    }

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: clean_crypto_bars.py <input_parquet> <output_catalog_dir> <symbol> [timeframe]")
        sys.exit(1)
    res = clean_and_audit_ohlcv(
        input_parquet_path=sys.argv[1],
        output_catalog_dir=sys.argv[2],
        symbol=sys.argv[3],
        timeframe=sys.argv[4] if len(sys.argv) > 4 else "1m"
    )
    print(json.dumps(res, indent=2))
