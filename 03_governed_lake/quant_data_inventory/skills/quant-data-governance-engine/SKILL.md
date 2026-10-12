---
name: quant-data-governance-engine
description: End-to-end data acquisition, integrity auditing, and governance engine for quantitative trading. Normalizes raw multi-asset OHLCV (Crypto 24/7, US Equities, Futures) into clean dual-compatible Parquet lakes and native Nautilus Trader ParquetDataCatalogs.
license: MIT
metadata:
  version: "1.0.0"
  frameworks: [nautilus_trader, polars, duckdb, pyarrow]
---

# Quant Data Governance Engine

Standard operating protocol and toolchain for quantitative market data engineering:
1. High-throughput resumable download from Hugging Face / GitHub with automatic domestic mirror routing and CIFS/SMB filelock handling.
2. Invariant verification ($H \ge \max(O,C)$, $L \le \min(O,C)$, $V \ge 0$, monotonic unique timestamps) via deterministic checks.
3. 24/7 Crypto gap auditing without synthetic forward-fill.
4. Export to clean dual-target lakes: universal research Parquet + native Nautilus Trader `ParquetDataCatalog`.

## Quick Start Commands

### 1. Download Datasets
```bash
python3 scripts/download_hf_dataset.py <repo_id> <target_dir> [max_workers]
```

### 2. Audit & Clean Intraday Crypto Bars
```bash
python3 scripts/clean_crypto_bars.py <input_parquet> <output_catalog_dir> <symbol> [timeframe]
```

### 3. Convert to Native Nautilus Trader DataCatalog
```bash
python3 scripts/nautilus_catalog_converter.py <clean_parquet> <catalog_dir> [symbol] [venue]
```

## Schema Standards

### Research Parquet Schema
- `timestamp`: `timestamp[us, tz=UTC]`
- `symbol`: `string`
- `timeframe`: `string` (`1m`, `5m`, `15m`, `1h`, `1d`)
- `bar_type`: `string` (`<symbol>.<venue>-<timeframe>-LAST-EXTERNAL`)
- `ts_event`: `uint64` (nanoseconds UTC)
- `ts_init`: `uint64` (nanoseconds UTC)
- `open`, `high`, `low`, `close`, `volume`: `float64`

### Nautilus Trader Parquet DataCatalog
- Native path: `<catalog_root>/data/bar/<bar_type>/<chunk>.parquet`
- Ingestion: via `nautilus_trader.persistence.wranglers.BarDataWrangler`
- Direct backtesting load: `catalog.bars(bar_types=[...])`
