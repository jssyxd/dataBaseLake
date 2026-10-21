"""run_iteration.py - Systematic Backtesting & Iteration Script

Evaluates the Macro Trend + DVOL Regime + Volume Surge Strategy.
"""

import polars as pl
import duckdb
from pathlib import Path
from engine import VectorizedBacktester, BacktestConfig
from strategy import generate_signals

BASE_DIR = Path(__file__).resolve().parents[2]
BTC_PATH = BASE_DIR / "03_governed_lake" / "v2_parquet_lake" / "crypto" / "futures_um" / "BTCUSDT" / "15m" / "BTCUSDT_15m_consolidated.parquet"
DVOL_PATH = BASE_DIR / "03_governed_lake" / "v2_parquet_lake" / "macro" / "dvol" / "BTCDVOL_1h_consolidated.parquet"

def load_and_align_data() -> pl.DataFrame:
    btc = pl.read_parquet(BTC_PATH)
    btc = btc.with_columns((pl.col("timestamp") * 1_000_000).alias("ts_event"))

    dvol = pl.read_parquet(DVOL_PATH)
    dvol = dvol.select([
        (pl.col("timestamp") + 3600 * 1_000_000_000).alias("dvol_available_ts"),
        pl.col("close").alias("dvol_close")
    ]).sort("dvol_available_ts")

    con = duckdb.connect()
    aligned = con.execute("""
        SELECT 
            b.timestamp,
            b.datetime,
            b.open, b.high, b.low, b.close, b.volume, b.taker_buy_volume,
            d.dvol_close
        FROM btc b
        ASOF JOIN dvol d
        ON b.ts_event >= d.dvol_available_ts
        ORDER BY b.ts_event
    """).pl()
    return aligned

def run_experiment_grid(df: pl.DataFrame):
    backtester = VectorizedBacktester(BacktestConfig(
        initial_capital=100_000.0,
        fee_rate=0.0005,       # 5 bps taker fee
        slippage=0.0002,       # 2 bps slippage
        funding_rate_8h=0.0001,
        target_vol=0.40,
        max_leverage=1.8
    ))

    bnh_return = (df["close"][-1] / df["open"][0] - 1.0) * 100
    peaks = df["close"].cum_max()
    bnh_dd = ((df["close"] - peaks) / peaks).min() * 100
    print("\n" + "="*85)
    print(f"BENCHMARK: Buy & Hold Return: {bnh_return:.2f}% | Max Drawdown: {bnh_dd:.2f}%")
    print("="*85)

    experiments = [
        # (channel, trend, cvd, dvol_cap, label)
        (96, 480, 24, 100.0, "24h Ch + 5d Trend + CVD (No DVOL)"),
        (96, 480, 24, 75.0, "24h Ch + 5d Trend + CVD (DVOL < 75%)"),
        (192, 960, 48, 100.0, "48h Ch + 10d Trend + CVD (No DVOL)"),
        (192, 960, 48, 75.0, "48h Ch + 10d Trend + CVD (DVOL < 75%)"),
        (288, 1440, 72, 100.0, "72h Ch + 15d Trend + CVD (No DVOL)"),
        (288, 1440, 72, 70.0, "72h Ch + 15d Trend + CVD (DVOL < 70%)"),
    ]

    results = []
    for ch, tr, cvd_p, dvol_cap, label in experiments:
        df_sig = generate_signals(
            df, 
            channel_period=ch, 
            trend_period=tr,
            cvd_period=cvd_p, 
            dvol_cap_pct=dvol_cap
        )
        _, m = backtester.run(df_sig)
        results.append({
            "Strategy / Variation": label,
            "CAGR (%)": m["CAGR (%)"],
            "Total Ret (%)": m["Total Return (%)"],
            "Max DD (%)": m["Max Drawdown (%)"],
            "Calmar": m["Calmar Ratio"],
            "Sharpe": m["Sharpe Ratio"],
            "Trades": m["Trade Count"]
        })

    res_table = pl.DataFrame(results)
    print("\nITERATIVE BACKTEST RESULTS MATRIX:")
    print(res_table)

    out_csv = Path(__file__).parent / "iteration_results.csv"
    res_table.write_csv(out_csv)
    print(f"\nReport written to {out_csv}")

if __name__ == "__main__":
    aligned_df = load_and_align_data()
    run_experiment_grid(aligned_df)
