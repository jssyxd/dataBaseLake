# CMC20 Index Quantitative Governance & Invariant Pipeline

## 1. Overview & Lineage
The **CoinMarketCap 20 Index (CMC20)** tracks the top 20 leading cryptocurrencies by market capitalization, representing over 85% of global crypto market volume.

- **Data Source**: CoinMarketCap Keyless Public API (`/v3/index/cmc20-historical`)
- **Native Granularity**: 15m intervals with 20 constituent assets breakdown (USD prices, units, weights)
- **Coverage**: 2024-01-01 to Present
- **Governed Output**: Apache Parquet (`zstd` compressed, level 7) across `15m`, `30m`, `1h`, `2h`, `4h`, `1d`
- **Signals**: Multi-factor quantitative signals including Dual EMA, MACD Momentum, RSI, Bollinger Breakouts, and cross-sectional **Market Breadth Thrust**.

## 2. Mathematical Invariants Audit Summary

| Invariant | Standard | Status | Verification Detail |
| :--- | :--- | :---: | :--- |
| **Monotonicity** | $ts_i > ts_{i-1}$ (UTC int64 ms) | **PASS** | 0 timestamp reversals across 97,604 bars |
| **Deduplication** | Zero collision / duplicates | **PASS** | 0 duplicate timestamps |
| **K-line Geometry** | $High \ge \max(Open, Close)$<br>$Low \le \min(Open, Close)$ | **PASS** | Evaluated on index OHLC and constituent OHLC |
| **Positive Prices** | $Open, High, Low, Close > 0$ | **PASS** | All values strictly $> 0$, 0 NaNs |
| **Non-negative Units** | $Volume \ge 0, Units \ge 0$ | **PASS** | Constituent units and weights non-negative |
| **Zero Synthetic Fill**| Real market hiatus preservation | **PASS** | 3 natural exchange gaps preserved without lookahead forward-fill |
| **Signal Domain** | Discrete $\{-1, 0, 1\}$ decisions | **PASS** | All signals bound to standard states |
