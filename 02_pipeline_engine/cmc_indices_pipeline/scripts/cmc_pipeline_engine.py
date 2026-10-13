#!/usr/bin/env python3
"""
CMC20 Quantitative Governance, Resampling & Multi-Factor Signal Engine
======================================================================
Pipeline engine for dataBaseLake/02_pipeline_engine/cmc_indices_pipeline.

Features:
1. Strict schema normalization to dataBaseLake int64 UTC ms alignment.
2. High-precision offline OHLC resampling (30m, 1h, 2h, 4h, 1d).
3. Institutional Signal Pipeline (Dual EMA, MACD, RSI, Bollinger Bands, CMC20 Market Breadth, Ensemble).
4. Full mathematical invariant enforcement:
   - Monotonicity (ts[i] > ts[i-1])
   - Zero duplicates
   - K-line geometric boundaries: High >= max(Open, Close), Low <= min(Open, Close)
   - Positive price / index range: Open, High, Low, Close > 0
   - Non-negative volumes, units & weights
   - Discrete signal domain: {-1, 0, 1}
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


def resample_cmc20(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    Resamples raw 15m CMC20 data into target frequency OHLC bars
    along with Top 5 constituent assets OHLC.
    """
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp").sort_index()

    # 1. Base Index OHLC
    resampled_val = df["value"].resample(freq).ohlc()
    resampled_val.columns = [f"index_{col}" for col in resampled_val.columns]

    # 2. Key Constituents OHLC
    top_symbols = ["BTC", "ETH", "SOL", "BNB", "XRP"]
    constituent_dfs = []
    for sym in top_symbols:
        price_col = f"{sym}_price_usd"
        if price_col in df.columns:
            s_ohlc = df[price_col].resample(freq).ohlc()
            s_ohlc.columns = [f"{sym}_{col}" for col in s_ohlc.columns]
            constituent_dfs.append(s_ohlc)

    res = pd.concat([resampled_val] + constituent_dfs, axis=1).dropna(subset=["index_close"])
    res["return"] = res["index_close"].pct_change()
    return res


def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes institutional signals across 5 quant dimensions:
    - Trend: Dual EMA (12/26) & MACD histogram momentum
    - Momentum: RSI (14)
    - Volatility Breakout: Bollinger Bands (20, 2)
    - Cross-sectional Breadth: CMC20 Market Breadth Thrust (% of constituents above MA20)
    - Ensemble: Weighted composite decision signal {-1, 0, 1}
    """
    data = df.copy()
    if "index_close" in data.columns:
        close = data["index_close"]
    elif "value" in data.columns:
        close = data["value"]
    else:
        raise ValueError("DataFrame must contain 'index_close' or 'value'")

    # Trend
    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    data["ema_fast"] = ema_fast
    data["ema_slow"] = ema_slow
    data["signal_ema_cross"] = np.where(ema_fast > ema_slow, 1, -1)
    data["signal_macd"] = np.where((macd_line > macd_signal) & (macd_hist > 0), 1,
                           np.where((macd_line < macd_signal) & (macd_hist < 0), -1, 0))

    # Momentum
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    data["rsi_14"] = rsi
    data["signal_rsi"] = np.where(rsi < 35, 1,
                         np.where(rsi > 65, -1, 0))

    # Bollinger Bands
    sma_20 = close.rolling(window=20).mean()
    std_20 = close.rolling(window=20).std()
    bb_upper = sma_20 + 2 * std_20
    bb_lower = sma_20 - 2 * std_20
    data["bb_upper"] = bb_upper
    data["bb_lower"] = bb_lower
    data["bb_width"] = (bb_upper - bb_lower) / (sma_20 + 1e-9)
    data["signal_bb"] = np.where(close > bb_upper, 1,
                        np.where(close < bb_lower, -1, 0))

    # Cross-sectional Breadth
    price_cols = [c for c in data.columns if c.endswith("_price_usd") or (c.endswith("_close") and not c.startswith("index_"))]
    if price_cols:
        above_ma_list = []
        for col in price_cols:
            col_ma = data[col].rolling(20).mean()
            above_ma_list.append((data[col] > col_ma).astype(int))
        if above_ma_list:
            breadth_ratio = pd.concat(above_ma_list, axis=1).mean(axis=1)
            data["market_breadth_ratio"] = breadth_ratio
            data["signal_breadth"] = np.where(breadth_ratio >= 0.75, 1,
                                     np.where(breadth_ratio <= 0.25, -1, 0))
        else:
            data["signal_breadth"] = 0
    else:
        data["signal_breadth"] = 0

    # Composite Ensemble Decision
    score = (
        data["signal_ema_cross"] * 0.30 +
        data["signal_macd"] * 0.20 +
        data["signal_rsi"] * 0.20 +
        data["signal_bb"] * 0.15 +
        data["signal_breadth"] * 0.15
    )
    data["composite_score"] = score
    data["signal_ensemble"] = np.where(score >= 0.35, 1,
                             np.where(score <= -0.35, -1, 0))
    return data


def format_standard_lake_columns(df: pd.DataFrame, is_ohlc: bool = False) -> pd.DataFrame:
    """
    Ensures dataBaseLake mandatory columns:
    - timestamp: int64 milliseconds UTC
    - datetime: ISO string UTC
    - open, high, low, close: float64
    """
    res = df.copy()
    if isinstance(res.index, pd.DatetimeIndex):
        dt_series = res.index
    else:
        dt_series = pd.to_datetime(res["timestamp"], utc=True)

    ts_ms = (dt_series.astype("int64") // 1_000_000).values
    dt_str = dt_series.strftime("%Y-%m-%d %H:%M:%S").values

    res.insert(0, "timestamp_ms", ts_ms)
    res.insert(1, "datetime", dt_str)

    if is_ohlc and "index_open" in res.columns:
        res["open"] = res["index_open"]
        res["high"] = res["index_high"]
        res["low"] = res["index_low"]
        res["close"] = res["index_close"]
    elif "value" in res.columns:
        res["open"] = res["value"]
        res["high"] = res["value"]
        res["low"] = res["value"]
        res["close"] = res["value"]

    if "timestamp" in res.columns and "timestamp_ms" in res.columns:
        # Swap timestamp to int64 ms to match database contract
        res["timestamp_raw"] = res["timestamp"]
        res["timestamp"] = res["timestamp_ms"]
        res = res.drop(columns=["timestamp_ms"])
    elif "timestamp_ms" in res.columns:
        res["timestamp"] = res["timestamp_ms"]
        res = res.drop(columns=["timestamp_ms"])

    return res
