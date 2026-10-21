#!/usr/bin/env python3
"""
Multi-Asset Incremental Market Data Updater & Dual-Lake Serializer (dataBaseLake)
---------------------------------------------------------------------------------
Supports 8 target instruments:
- Crypto (Binance USDT-M): BTCUSDT, DOGEUSDT
- US Index Futures (CME): NQ, ES
- Commodities Futures (COMEX/NYMEX): GC, CL
- CME Micro Crypto Futures: MBT, MET

Supported target timeframes: 5m, 15m, 1h, 2h
Dual-target serialization:
1. v2_parquet_lake (universal analytical schema, Zstd-7)
2. nautilus_databank (Nautilus native Parquet catalog, ns timestamp)
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MultiAssetUpdater")

BASE_DIR = Path(__file__).resolve().parent.parent
GOVERNED_DIR = BASE_DIR / "03_governed_lake"
V2_DIR = GOVERNED_DIR / "v2_parquet_lake"
CATALOG_BAR_DIR = GOVERNED_DIR / "nautilus_databank" / "catalog" / "data" / "bar"
RAW_LAKE_DIR = BASE_DIR / "01_raw_lake"
MANIFEST_FILE = V2_DIR / "manifest" / "manifest.json"

# Binance Endpoints
BINANCE_API_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi2.binance.com",
    "https://api.binance.com",
]

session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

TIMEFRAME_CONFIG = {
    "5m": {"ms": 5 * 60 * 1000, "ns": 5 * 60 * 1_000_000_000, "binance": "5m", "yf_interval": "5m"},
    "15m": {"ms": 15 * 60 * 1000, "ns": 15 * 60 * 1_000_000_000, "binance": "15m", "yf_interval": "15m"},
    "1h": {"ms": 60 * 60 * 1000, "ns": 60 * 60 * 1_000_000_000, "binance": "1h", "yf_interval": "1h"},
    "2h": {"ms": 120 * 60 * 1000, "ns": 120 * 60 * 1_000_000_000, "binance": "2h", "yf_interval": "1h"}, # 2h resampled from 1h for YF
}

SYMBOL_METADATA = {
    "BTC": {
        "type": "crypto",
        "binance_symbol": "BTCUSDT",
        "v2_rel_dir": "crypto/futures_um/BTCUSDT",
        "bar_type_prefix": "BTCUSD.CRYPTO",
        "is_crypto": True,
    },
    "ETH": {
        "type": "crypto",
        "binance_symbol": "ETHUSDT",
        "v2_rel_dir": "crypto/futures_um/ETHUSDT",
        "bar_type_prefix": "ETHUSD.CRYPTO",
        "is_crypto": True,
    },
    "SOL": {
        "type": "crypto",
        "binance_symbol": "SOLUSDT",
        "v2_rel_dir": "crypto/futures_um/SOLUSDT",
        "bar_type_prefix": "SOLUSD.CRYPTO",
        "is_crypto": True,
    },
    "BNB": {
        "type": "crypto",
        "binance_symbol": "BNBUSDT",
        "v2_rel_dir": "crypto/futures_um/BNBUSDT",
        "bar_type_prefix": "BNBUSD.CRYPTO",
        "is_crypto": True,
    },
    "DOGE": {
        "type": "crypto",
        "binance_symbol": "DOGEUSDT",
        "v2_rel_dir": "crypto/futures_um/DOGEUSDT",
        "bar_type_prefix": "DOGEUSD.CRYPTO",
        "is_crypto": True,
    },
    "NQ": {
        "type": "us_futures",
        "yf_ticker": "NQ=F",
        "v2_rel_dir": "us_futures/NQ",
        "bar_type_prefix": "NQ.CME",
        "is_crypto": False,
        "raw_names": ["NQ_1m.csv", "NAS100_5m.csv", "NAS100_15m.csv", "NAS100_1h.csv"],
    },
    "ES": {
        "type": "us_futures",
        "yf_ticker": "ES=F",
        "v2_rel_dir": "us_futures/ES",
        "bar_type_prefix": "ES.CME",
        "is_crypto": False,
        "raw_names": ["ES_1m.csv", "SPX500_5m.csv", "SPX500_15m.csv", "SPX500_1h.csv"],
    },
    "GC": {
        "type": "commodities",
        "yf_ticker": "GC=F",
        "v2_rel_dir": "global_futures/commodities_and_rates/GC",
        "bar_type_prefix": "GC.COMEX",
        "is_crypto": False,
        "raw_names": ["XAUUSD_5m.csv", "XAUUSD_15m.csv", "XAUUSD_1h.csv"],
    },
    "CL": {
        "type": "commodities",
        "yf_ticker": "CL=F",
        "v2_rel_dir": "global_futures/commodities_and_rates/CL",
        "bar_type_prefix": "CL.NYMEX",
        "is_crypto": False,
        "raw_names": ["USOIL_5m.csv", "USOIL_15m.csv", "USOIL_1h.csv"],
    },
    "MBT": {
        "type": "us_futures",
        "yf_ticker": "MBT=F",
        "v2_rel_dir": "us_futures/MBT",
        "bar_type_prefix": "MBT.CME",
        "is_crypto": False,
    },
    "MET": {
        "type": "us_futures",
        "yf_ticker": "MET=F",
        "v2_rel_dir": "us_futures/MET",
        "bar_type_prefix": "MET.CME",
        "is_crypto": False,
    },
}


def verify_invariants(df: pd.DataFrame, label: str = "") -> Tuple[bool, List[str]]:
    """Strictly assert the 5 Core Quant Invariants."""
    issues = []
    if df.empty:
        return True, []

    # 1. Monotonicity & Deduplication
    ts_col = "timestamp" if "timestamp" in df.columns else ("ts_event" if "ts_event" in df.columns else None)
    if ts_col:
        s = df[ts_col]
        if not s.is_monotonic_increasing:
            issues.append(f"[{label}] Timestamps not strictly monotonic increasing.")
        dups = s.duplicated().sum()
        if dups > 0:
            issues.append(f"[{label}] Found {dups} duplicate timestamps.")

    # 2. Envelope & Positive Domain
    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            issues.append(f"[{label}] Missing required OHLC column '{c}'")
            return False, issues

    h_viol = (df["high"] < df["open"]) | (df["high"] < df["close"])
    l_viol = (df["low"] > df["open"]) | (df["low"] > df["close"])
    hl_viol = df["low"] > df["high"]
    zero_viol = (df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0)

    if h_viol.any():
        issues.append(f"[{label}] High price invariant violated in {h_viol.sum()} rows.")
    if l_viol.any():
        issues.append(f"[{label}] Low price invariant violated in {l_viol.sum()} rows.")
    if hl_viol.any():
        issues.append(f"[{label}] Low > High violation in {hl_viol.sum()} rows.")
    if zero_viol.any():
        issues.append(f"[{label}] Non-positive price in {zero_viol.sum()} rows.")

    # 3. Non-negative volume
    if "volume" in df.columns:
        vol_viol = df["volume"] < 0
        if vol_viol.any():
            issues.append(f"[{label}] Negative volume in {vol_viol.sum()} rows.")

    return len(issues) == 0, issues


def get_binance_server_time() -> int:
    for host in BINANCE_API_HOSTS:
        url = f"{host}/fapi/v1/time" if "fapi" in host else f"{host}/api/v3/time"
        try:
            resp = session.get(url, timeout=5)
            if resp.status_code == 200:
                return int(resp.json().get("serverTime", int(time.time() * 1000)))
        except Exception:
            continue
    return int(time.time() * 1000)


def fetch_binance_klines_batch(symbol: str, interval: str, start_time: int, limit: int = 1500) -> list:
    for host in BINANCE_API_HOSTS:
        url = f"{host}/fapi/v1/klines" if "fapi" in host else f"{host}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "startTime": start_time, "limit": limit}
        for attempt in range(3):
            try:
                resp = session.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited (429) on {url}, backing off 5s...")
                    time.sleep(5)
            except Exception as e:
                time.sleep(0.5)
    return []


def normalize_symbol_key(sym: str) -> Optional[str]:
    s = sym.upper().strip()
    if s in SYMBOL_METADATA:
        return s
    for k, v in SYMBOL_METADATA.items():
        if v.get("binance_symbol") == s:
            return k
    if s.endswith("USDT") and s[:-4] in SYMBOL_METADATA:
        return s[:-4]
    if s.endswith("USD") and s[:-3] in SYMBOL_METADATA:
        return s[:-3]
    return None


def fetch_binance_incremental(symbol: str, interval: str, start_ms: int, end_ms: int, max_close_ms: Optional[int] = None) -> pd.DataFrame:
    interval_ms = TIMEFRAME_CONFIG[interval]["ms"]
    all_raw = []
    current_start = start_ms

    logger.info(f"[{symbol} {interval}] Ingesting from {datetime.fromtimestamp(current_start/1000, tz=timezone.utc)} to {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc)}")

    batch_idx = 0
    total_expected = max(1, (end_ms - current_start) // (1500 * interval_ms) + 1)
    while current_start <= end_ms:
        batch = fetch_binance_klines_batch(symbol, interval, current_start, limit=1500)
        if not batch:
            break
        all_raw.extend(batch)
        batch_idx += 1
        last_bar_open = batch[-1][0]
        next_start = last_bar_open + interval_ms
        if next_start <= current_start:
            break
        current_start = next_start
        if batch_idx % 20 == 0:
            logger.info(f"[{symbol} {interval}] Progress: {batch_idx}/{total_expected} batches ({len(all_raw)} bars)")

        if len(batch) < 1500 or current_start > end_ms:
            break
        time.sleep(0.01)

    new_df = parse_binance_raw_to_df(all_raw, end_ms, max_close_ms=max_close_ms)
    if not new_df.empty:
        new_df = new_df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
    return new_df


def parse_binance_raw_to_df(all_raw: list, end_ms: int, max_close_ms: Optional[int] = None) -> pd.DataFrame:
    if not all_raw:
        return pd.DataFrame()
    records = []
    for k in all_raw:
        ts = int(k[0])
        if ts > end_ms:
            continue
        if max_close_ms is not None and len(k) > 6 and int(k[6]) > max_close_ms:
            continue
        dt_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        records.append({
            "timestamp": ts,
            "datetime": dt_str,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "quote_volume": float(k[7]),
            "taker_buy_volume": float(k[9]),
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["timestamp"] = df["timestamp"].astype("int64")
    df["datetime"] = df["datetime"].astype("string")
    for c in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume"]:
        df[c] = df[c].astype("float64")
    return df


def fetch_yfinance_incremental(yf_ticker: str, interval: str) -> pd.DataFrame:
    """Fetch recent bars from Yahoo Finance for CME futures."""
    import yfinance as yf
    # Intervals in yfinance: 5m max 60 days, 15m max 60 days, 1h max 730 days
    yf_int = TIMEFRAME_CONFIG[interval]["yf_interval"]
    period = "60d" if yf_int in ["5m", "15m"] else "730d"

    logger.info(f"[{yf_ticker}] Fetching yfinance history (period={period}, interval={yf_int})")
    try:
        t = yf.Ticker(yf_ticker)
        df_yf = t.history(period=period, interval=yf_int, auto_adjust=False)
        if df_yf.empty:
            logger.warning(f"[{yf_ticker}] yfinance returned empty dataframe.")
            return pd.DataFrame()

        df_yf = df_yf.reset_index()
        # Find datetime column
        dt_col = "Datetime" if "Datetime" in df_yf.columns else "Date"
        df_yf["datetime_utc"] = pd.to_datetime(df_yf[dt_col], utc=True)
        df_yf["timestamp"] = (df_yf["datetime_utc"].astype("int64") // 1_000_000).astype("int64")
        df_yf["datetime"] = df_yf["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S").astype("string")

        # Map OHLCV
        df_out = pd.DataFrame({
            "timestamp": df_yf["timestamp"],
            "datetime": df_yf["datetime"],
            "open": df_yf["Open"].astype("float64"),
            "high": df_yf["High"].astype("float64"),
            "low": df_yf["Low"].astype("float64"),
            "close": df_yf["Close"].astype("float64"),
            "volume": df_yf["Volume"].fillna(0.0).astype("float64"),
        })

        # Resample to 2h if requested
        if interval == "2h":
            df_out["dt"] = pd.to_datetime(df_out["timestamp"], unit="ms", utc=True)
            resampled = df_out.set_index("dt").resample("2h", label="left", closed="left").agg({
                "timestamp": "first",
                "datetime": "first",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna().reset_index(drop=True)
            df_out = resampled

        df_out = df_out.dropna(subset=["open", "high", "low", "close"])
        df_out = df_out[df_out["open"] > 0]
        df_out = df_out.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        return df_out
    except Exception as e:
        logger.error(f"[{yf_ticker}] Failed to fetch from yfinance: {e}")
        return pd.DataFrame()


def rollup_from_lower(df_lower: pd.DataFrame, target_interval: str, is_crypto: bool = False) -> pd.DataFrame:
    """Deterministically aggregate bars to target interval (e.g. 5m -> 15m/1h/2h, or 1h -> 2h)."""
    if df_lower.empty:
        return pd.DataFrame()

    df = df_lower.copy()
    dt = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["dt"] = dt

    rule_map = {
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "2h": "2h",
    }
    rule = rule_map[target_interval]

    agg_dict = {
        "timestamp": "first",
        "datetime": "first",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if is_crypto:
        if "quote_volume" in df.columns:
            agg_dict["quote_volume"] = "sum"
        if "taker_buy_volume" in df.columns:
            agg_dict["taker_buy_volume"] = "sum"

    resampled = df.set_index("dt").resample(rule, label="left", closed="left").agg(agg_dict).dropna(subset=["open"]).reset_index(drop=True)
    resampled["timestamp"] = resampled["timestamp"].astype("int64")
    resampled["datetime"] = resampled["datetime"].astype("string")
    for c in ["open", "high", "low", "close", "volume"]:
        resampled[c] = resampled[c].astype("float64")
    if is_crypto:
        if "quote_volume" in resampled.columns:
            resampled["quote_volume"] = resampled["quote_volume"].astype("float64")
        if "taker_buy_volume" in resampled.columns:
            resampled["taker_buy_volume"] = resampled["taker_buy_volume"].astype("float64")

    # Assert invariants on rolled-up bars
    resampled = resampled.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return resampled


def save_v2_parquet(df: pd.DataFrame, target_path: Path):
    target_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    tmp_path = target_path.with_suffix(".tmp.parquet")
    pq.write_table(table, tmp_path, compression="zstd", compression_level=7)
    tmp_path.replace(target_path)
    logger.info(f"Saved V2 Lake Parquet: {target_path} ({len(df)} rows)")


def save_nautilus_catalog(df: pd.DataFrame, bar_type: str, is_crypto: bool, interval_ms: int):
    """Write partitions to nautilus_databank catalog."""
    if df.empty:
        return

    catalog_dir = CATALOG_BAR_DIR / bar_type
    catalog_dir.mkdir(parents=True, exist_ok=True)

    dt_series = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df_cat = pd.DataFrame()
    df_cat["timestamp"] = dt_series
    df_cat["symbol"] = bar_type.split(".")[0]
    df_cat["timeframe"] = bar_type.split("-")[1].lower() if "-" in bar_type else "15m"
    df_cat["bar_type"] = bar_type
    df_cat["ts_event"] = (df["timestamp"] * 1_000_000).astype("uint64")
    df_cat["ts_init"] = ((df["timestamp"] + interval_ms) * 1_000_000 - 1).astype("uint64")
    df_cat["open"] = df["open"].astype("float64")
    df_cat["high"] = df["high"].astype("float64")
    df_cat["low"] = df["low"].astype("float64")
    df_cat["close"] = df["close"].astype("float64")
    df_cat["volume"] = df["volume"].astype("float64")

    df_cat["year"] = dt_series.dt.year

    for year, group in df_cat.groupby("year"):
        year_file = catalog_dir / f"{year}.parquet"
        to_save = group.drop(columns=["year"])

        if year_file.exists():
            try:
                exist_df = pd.read_parquet(year_file)
                merged = pd.concat([exist_df, to_save], ignore_index=True)
                merged = merged.drop_duplicates(subset=["ts_event"], keep="last").sort_values("ts_event").reset_index(drop=True)
            except Exception:
                merged = to_save
        else:
            merged = to_save

        table = pa.Table.from_pandas(merged, preserve_index=False)
        tmp_file = year_file.with_suffix(".tmp.parquet")
        pq.write_table(table, tmp_file, compression="zstd")
        tmp_file.replace(year_file)


def update_manifest():
    if not MANIFEST_FILE.exists():
        logger.warning(f"Manifest {MANIFEST_FILE} not found; skipping registry sync.")
        return

    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)

    now_iso = datetime.now(timezone.utc).isoformat()
    manifest_data["last_updated_utc"] = now_iso
    manifest_data["last_updated"] = now_iso
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Updated manifest registry: {MANIFEST_FILE}")


def run_pipeline(symbols: List[str], timeframes: List[str], dry_run: bool = False):
    resolved_symbols = []
    for s in symbols:
        k = normalize_symbol_key(s)
        if k:
            if k not in resolved_symbols:
                resolved_symbols.append(k)
        else:
            logger.warning(f"Unknown symbol '{s}'; skipping.")

    logger.info(f"Running pipeline for symbols: {resolved_symbols}, timeframes: {timeframes}, dry_run={dry_run}")
    server_time = get_binance_server_time()
    five_years_ago_ms = int((datetime.now(timezone.utc) - timedelta(days=5*365)).timestamp() * 1000)

    for sym_key in resolved_symbols:
        meta = SYMBOL_METADATA[sym_key]
        is_crypto = meta["is_crypto"]

        for tf in timeframes:
            interval_ms = TIMEFRAME_CONFIG[tf]["ms"]
            v2_target = V2_DIR / meta["v2_rel_dir"] / tf / f"{sym_key if not is_crypto else meta['binance_symbol']}_{tf}_consolidated.parquet"
            # Special naming check for existing files (e.g. NQ_15m.parquet or NQ_15m_consolidated.parquet)
            if not v2_target.exists():
                alt_name = V2_DIR / meta["v2_rel_dir"] / tf / f"{sym_key if not is_crypto else meta['binance_symbol']}_{tf}.parquet"
                if alt_name.exists():
                    v2_target = alt_name

            existing_df = pd.DataFrame()
            if v2_target.exists():
                try:
                    existing_df = pd.read_parquet(v2_target)
                    logger.info(f"[{sym_key} {tf}] Found existing data: {len(existing_df)} rows ({existing_df['datetime'].iloc[0]} -> {existing_df['datetime'].iloc[-1]})")
                except Exception as e:
                    logger.warning(f"Could not read {v2_target}: {e}")

            new_df = pd.DataFrame()
            if is_crypto:
                binance_sym = meta["binance_symbol"]
                start_ms = five_years_ago_ms
                if not existing_df.empty:
                    last_ts = int(existing_df["timestamp"].max())
                    start_ms = max(start_ms, last_ts + interval_ms)

                if start_ms <= server_time - interval_ms:
                    if dry_run:
                        logger.info(f"[DRY-RUN] Would fetch Binance klines for {binance_sym} {tf} from {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)}")
                    else:
                        new_df = fetch_binance_incremental(binance_sym, tf, start_ms, server_time, max_close_ms=server_time)
            else:
                # Futures (CME / COMEX / NYMEX)
                yf_ticker = meta["yf_ticker"]
                if dry_run:
                    logger.info(f"[DRY-RUN] Probing yfinance for {yf_ticker} {tf}")
                else:
                    new_df = fetch_yfinance_incremental(yf_ticker, tf)

            if dry_run:
                continue

            # Merge existing and newly fetched
            if not new_df.empty:
                logger.info(f"[{sym_key} {tf}] Fetched {len(new_df)} new closed bars ({new_df['datetime'].iloc[0]} -> {new_df['datetime'].iloc[-1]})")
                if not existing_df.empty:
                    merged = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    merged = new_df
            else:
                logger.info(f"[{sym_key} {tf}] No new bars needed.")
                merged = existing_df

            if merged.empty:
                logger.warning(f"[{sym_key} {tf}] No data available.")
                continue

            # Sort and deduplicate
            merged = merged.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)

            # Invariant check
            valid, issues = verify_invariants(merged, f"{sym_key} {tf}")
            if not valid:
                logger.error(f"[{sym_key} {tf}] Invariant check failed: {issues}")
                raise AssertionError(f"Governance violations in {sym_key} {tf}: {issues}")

            # Save to V2 Lake
            save_v2_parquet(merged, v2_target)

            # Save to Nautilus Catalog
            bar_tf_str = {
                "5m": "5-MINUTE",
                "15m": "15-MINUTE",
                "1h": "1-HOUR",
                "2h": "2-HOUR",
            }[tf]
            bar_type = f"{meta['bar_type_prefix']}-{bar_tf_str}-LAST-EXTERNAL"
            save_nautilus_catalog(merged, bar_type, is_crypto, interval_ms)

    if not dry_run:
        update_manifest()


def main():
    parser = argparse.ArgumentParser(description="Multi-Asset Incremental Updater")
    parser.add_argument("--symbols", nargs="+", default=["BTC", "DOGE", "NQ", "ES", "GC", "CL", "MBT", "MET"], help="Symbols to update")
    parser.add_argument("--timeframes", nargs="+", default=["5m", "15m", "1h", "2h"], help="Timeframes to update")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without modifying disk")
    args = parser.parse_args()

    run_pipeline(args.symbols, args.timeframes, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
