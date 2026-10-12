# Quant Data Governance Methodology & Standards

## 1. Physical Storage Architecture & Filesystem Quirks
- **Large-scale Lake Storage**: When local root partitions are constrained (e.g. 50GB SSD), network/SMB volumes must be utilized.
- **CIFS/GVFS Mount Limitations**: GVFS and SMB mounts do not support POSIX `chmod`/`fchmod` mode changes or UNIX domain file locks. `filelock` and downloader processes must intercept and suppress `OSError(Errno 95 EOPNOTSUPP)`.
- **Memory Scaling**: Large-scale downloads and multi-GB Parquet aggregations require a minimum 1:1 Swap allocation (e.g. 12GB RAM -> 12GB `/swap.img`) configured permanently in `/etc/fstab`.

## 2. Network Routing (China Domestic Mirror vs Overseas Proxy)
- `hf-mirror.com` resides in mainland China.
- Routing requests to `hf-mirror.com` through an overseas proxy (`192.168.1.5:7890`) causes cross-border round-trip loops, triggering SSL handshake EOF drops and protocol timeouts.
- **Rule**: Direct domestic connection (`unset http_proxy https_proxy all_proxy`) for `hf-mirror.com`. Use proxy ONLY for GitHub repositories and token verification against `huggingface.co`.

## 3. Data Integrity & Validation Invariants
Every OHLCV panel must strictly enforce:
1. **Timestamp Monotonicity**: `ts[i] > ts[i-1]` strictly ascending.
2. **Deduplication**: Exact duplicate timestamps must be removed, keeping the last verified record.
3. **OHLC Price Consistency**:
   - $High \ge \max(Open, Close)$
   - $Low \le \min(Open, Close)$
   - $Low \le High$
   - $Open, High, Low, Close > 0$
4. **Volume Invariant**: $Volume \ge 0$.
5. **Gap Detection**:
   - **Crypto (24/7 continuous)**: No market halts. Any gap $> 1.5 \times \text{expected\_interval}$ is logged to `defects.json` as missing market liquidity / exchange outage. **NEVER silently forward-fill** with synthetic prices.
   - **Equities & Futures**: Must account for official exchange trading calendars and market sessions.

## 4. Nautilus Trader Native Ingestion Standard
- Nautilus Trader uses high-precision 128-bit decimal fixed binary types (`fixed_size_binary[16]`) for Price and Quantity in its internal Rust catalog.
- Canonical target format preserves both standard float64 columns (`open, high, low, close, volume, timestamp, symbol, timeframe`) for general research, plus `bar_type`, `ts_event`, and `ts_init` (uint64 nanoseconds).
- Seamless ingestion is achieved using `BarDataWrangler(bar_type, instrument)` with `ParquetDataCatalog.write_data()`.
