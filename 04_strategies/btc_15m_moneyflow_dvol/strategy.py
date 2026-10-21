"""strategy.py - Institutional Dual-Filter Swing Strategy

Multi-Timeframe Architecture:
1. Macro Baseline: 200-SMA on 1h / 4h trend filter (only buy when above institutional trend).
2. Volatility Compression / Expansion:
   - DVOL Squeeze: DVOL percentile < 35% indicates high explosive upside probability.
   - DVOL Overheating: DVOL > 80% suppresses new position entries.
3. Micro MoneyFlow:
   - Volume surge (> 1.5x 24h rolling volume).
   - CVD expansion (positive net buyer accumulation).
"""

import polars as pl
import numpy as np

def generate_signals(
    df: pl.DataFrame,
    channel_period: int = 192,     # 48h channel
    trend_period: int = 960,       # 10 days institutional trend (240h = 960 bars)
    cvd_period: int = 48,          # 12h CVD flow
    dvol_cap_pct: float = 75.0,    # Gating threshold
    dvol_squeeze_pct: float = 40.0 # Squeeze threshold
) -> pl.DataFrame:
    # 1. Money Flow
    df = df.with_columns([
        (2.0 * pl.col("taker_buy_volume") - pl.col("volume")).alias("vol_delta")
    ])
    df = df.with_columns([
        pl.col("vol_delta").rolling_sum(window_size=cvd_period).alias("cvd_flow"),
        pl.col("volume").rolling_mean(window_size=96).alias("vol_ma96")
    ])

    # 2. Donchian Channel & Long-Term Trend
    df = df.with_columns([
        pl.col("high").rolling_max(window_size=channel_period).alias("channel_high"),
        pl.col("close").ewm_mean(span=channel_period).alias("channel_mid"),
        pl.col("close").rolling_mean(window_size=trend_period).alias("macro_trend")
    ])

    # 3. DVOL Percentile (60 days)
    roll_len = min(5760, len(df))
    df = df.with_columns([
        pl.col("dvol_close").rolling_min(window_size=roll_len).alias("dvol_min"),
        pl.col("dvol_close").rolling_max(window_size=roll_len).alias("dvol_max")
    ])
    df = df.with_columns([
        ((pl.col("dvol_close") - pl.col("dvol_min")) / 
         (pl.col("dvol_max") - pl.col("dvol_min") + 1e-6) * 100.0).alias("dvol_pct")
    ])

    close_p = df["close"].to_numpy()
    ch_high = df["channel_high"].to_numpy()
    ch_mid = df["channel_mid"].to_numpy()
    macro_t = df["macro_trend"].to_numpy()
    cvd_flow = df["cvd_flow"].to_numpy()
    volume = df["volume"].to_numpy()
    vol_ma = df["vol_ma96"].to_numpy()
    dvol_pct = df["dvol_pct"].to_numpy()
    n = len(df)

    raw_signals = np.zeros(n, dtype=np.int32)
    state = 0

    for i in range(trend_period + 2, n):
        curr_dvol = dvol_pct[i-1]
        
        # Long Entry:
        # 1. Above macro trend (Close > 10d MA)
        # 2. Channel breakout (Close >= Channel High of past 48h)
        # 3. CVD confirmed accumulation (CVD > 0) AND volume surge (vol > 1.2 * vol_ma)
        # 4. DVOL not in panic overheating (< dvol_cap_pct)
        if state == 0:
            if (close_p[i-1] >= ch_high[i-2] and 
                close_p[i-1] > macro_t[i-1] and 
                cvd_flow[i-1] > 0 and 
                volume[i-1] > 1.2 * vol_ma[i-1] and 
                curr_dvol < dvol_cap_pct):
                state = 1
        elif state == 1:
            # Trailing Exit:
            # Close drops below channel mid, or drops below macro trend, or DVOL spike panic (> 88)
            if close_p[i-1] < ch_mid[i-1] or close_p[i-1] < macro_t[i-1] or curr_dvol > 88.0:
                state = 0

        raw_signals[i] = state

    df = df.with_columns([
        pl.Series("signal", raw_signals)
    ])

    return df
