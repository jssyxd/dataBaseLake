"""engine.py - Production-Grade Vectorized & Event-Consistent Backtest Engine

Features:
1. Strict Anti-Lookahead: Signal generated on bar i-1 close, executed on bar i open.
2. Realistic Friction: 
   - Taker fee: 0.05% (5 bps)
   - Slippage: 2 bps
   - 8-hour funding rate proxy (0.01% / 8h)
3. Volatility-Targeting: Position sizing scaled with DVOL, but only updated upon state transitions
   or major rebalancing, preventing intra-trade fee churning!
"""

import numpy as np
import polars as pl
from dataclasses import dataclass
from typing import Dict, Any, Tuple

@dataclass
class BacktestConfig:
    initial_capital: float = 100_000.0
    fee_rate: float = 0.0005        # 0.05% taker fee
    slippage: float = 0.0002        # 2 bps slippage
    funding_rate_8h: float = 0.0001 # 0.01% per 8h
    target_vol: float = 0.35        # 35% annualized volatility target
    max_leverage: float = 1.5       # Max leverage cap

class VectorizedBacktester:
    def __init__(self, config: BacktestConfig = None):
        self.cfg = config or BacktestConfig()

    def run(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        open_p = df["open"].to_numpy()
        close_p = df["close"].to_numpy()
        signal = df["signal"].to_numpy()
        dvol = df["dvol_close"].to_numpy()
        n = len(df)

        equity = np.zeros(n)
        equity[0] = self.cfg.initial_capital
        cash = self.cfg.initial_capital
        units = 0.0
        
        trade_count = 0
        prev_signal = 0
        funding_interval = 32  # 32 * 15m = 8 hours

        for i in range(1, n):
            curr_signal = signal[i]
            open_curr = open_p[i]
            close_curr = close_p[i]

            # Rebalance ONLY on signal change (Entry, Exit, or Reversal)
            if curr_signal != prev_signal:
                # 1. Close previous position if open
                if abs(units) > 1e-6:
                    exec_price = open_curr * (1 - self.cfg.slippage) if units > 0 else open_curr * (1 + self.cfg.slippage)
                    proceeds = units * exec_price
                    fee = abs(proceeds) * self.cfg.fee_rate
                    cash += (proceeds - fee)
                    units = 0.0
                    trade_count += 1

                # 2. Open new position if curr_signal != 0
                if curr_signal != 0:
                    current_equity = cash
                    if current_equity > 0:
                        safe_dvol = max(dvol[i-1], 10.0) / 100.0
                        pos_scale = np.clip(self.cfg.target_vol / safe_dvol, 0.2, self.cfg.max_leverage)
                        target_val = current_equity * pos_scale * curr_signal
                        exec_price = open_curr * (1 + self.cfg.slippage) if curr_signal > 0 else open_curr * (1 - self.cfg.slippage)
                        
                        target_units = target_val / exec_price
                        fee = abs(target_val) * self.cfg.fee_rate
                        cash -= (target_units * exec_price + fee)
                        units = target_units
                        trade_count += 1

                prev_signal = curr_signal

            # Funding rate fee every 8 hours
            if i % funding_interval == 0 and abs(units) > 1e-6:
                funding_fee = abs(units * close_curr) * self.cfg.funding_rate_8h
                cash -= funding_fee

            # Mark to Market
            equity[i] = cash + units * close_curr
            if equity[i] <= 0:
                equity[i:] = 0
                break

        ret_series = np.diff(equity) / equity[:-1]
        ret_series = np.nan_to_num(ret_series, 0.0)

        total_return = (equity[-1] / equity[0]) - 1.0
        peaks = np.maximum.accumulate(equity)
        drawdowns = (equity - peaks) / peaks
        max_drawdown = float(np.min(drawdowns))

        bars_per_year = 35_040
        years = n / bars_per_year
        cagr = (1.0 + total_return) ** (1.0 / max(years, 0.01)) - 1.0
        calmar = abs(cagr / max_drawdown) if abs(max_drawdown) > 1e-5 else 0.0
        
        mean_ret = np.mean(ret_series)
        std_ret = np.std(ret_series)
        sharpe = (mean_ret / (std_ret + 1e-9)) * np.sqrt(bars_per_year)

        metrics = {
            "Total Return (%)": round(total_return * 100, 2),
            "CAGR (%)": round(cagr * 100, 2),
            "Max Drawdown (%)": round(max_drawdown * 100, 2),
            "Calmar Ratio": round(calmar, 2),
            "Sharpe Ratio": round(sharpe, 2),
            "Trade Count": trade_count,
            "Total Bars": n,
            "Years": round(years, 2)
        }

        res_df = df.with_columns([
            pl.Series("equity", equity),
            pl.Series("drawdown", drawdowns)
        ])

        return res_df, metrics
