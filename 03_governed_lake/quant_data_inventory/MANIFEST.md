# Quantitative Trading & OHLCV Datasets Manifest

Generated Date: 2026-09-24  
Target Root Directory: `~/quant-data/`  
Physical Mount Target: `/run/user/1000/gvfs/smb-share:server=192.168.1.6,share=iflow/数据库5/quant-data`  

---

## 1. Executive Summary

- **Total Acquired Datasets**: 9 (3 GitHub repositories + 6 Hugging Face Datasets)
- **Total Local Files**: **11,819 files**
- **Total Verified Storage**: **149.95 GB** (161,012,834,305 bytes)
- **Data Integrity & Governance**: All datasets downloaded with strict resume & checksum verification; per-dataset `DOWNLOAD_LOG.md` and `inventory.json` generated. No data cleaning/schema modification performed.

---

## 2. Dataset Inventory Breakdown

### Wave B: GitHub Repositories (Code & Historical Data)

| Dataset / Repository | Path | Files | Size | Scope & Description | Status |
|---|---|---|---|---|---|
| `JasonleeQAQ/multi-asset-ohlcv` | `github/multi-asset-ohlcv` | 31 | 47.26 KB | Multi-asset crypto/FX/stock OHLCV scripts & samples | **SUCCESS** |
| `stefan-jansen/machine-learning-for-trading` | `github/machine-learning-for-trading` | 124 | 11.32 MB | ML for Trading textbook repository (`data/` and `scripts/` sparse clone) | **SUCCESS** |
| `cenkcorapci/download-lots-of-financial-data` | `github/download-lots-of-financial-data` | 52 | 125.46 KB | Modular download scripts for global equities & commodities | **SUCCESS** |

### Wave C: Hugging Face Datasets (Large Scale OHLCV & Microstructure)

| Dataset | Path | Files | Size | Format & Timeframe | Status |
|---|---|---|---|---|---|
| `mito0o852/OHLCV-1m` | `hf/OHLCV-1m` | 413 | **81.70 GB** | US Equities 1-minute OHLCV (1992–2026 full monthly shards, Parquet) | **SUCCESS** |
| `twelvedata/financial-world-model` | `hf/financial-world-model` | 28 | **47.23 GB** | Global multi-asset bars (1d, 1h, 1m) + text metadata + trajectories | **SUCCESS** |
| `123olp/binance-futures-ohlcv-2018-2026` | `hf/binance-futures-ohlcv-2018-2026` | 13 | **16.06 GB** | Binance USDT-M Futures 1m candles + metrics + DB dumps (2018–2026) | **SUCCESS** |
| `OMCHOKSI108/my-cloud-data-lake` | `hf/my-cloud-data-lake` | 795 | **4.33 GB** | Multi-asset crypto/forex/indices (1m, 15m, 30m, 1h, 1d, Parquet) | **SUCCESS** |
| `zoanana990/qtrade-financial-data` | `hf/qtrade-financial-data` | 3 | **499.51 MB** | Quant trade financial features & parquet shards | **SUCCESS** |
| `yifishbossman/financial-analyst-data-demo` | `hf/financial-analyst-data-demo` | 10,360 | **137.87 MB** | Complete Qlib binary feature store (A-share OHLCV, volume, turnover) | **SUCCESS** |

---

## 3. Tooling & Environment Configuration

- **Hugging Face Hub (`hf`)**:
  - Full Access Token authenticated as `opclown1`.
  - Domestic Mirror: `HF_ENDPOINT=https://hf-mirror.com` (direct connection without overseas proxy to prevent SSL drops).
- **GitHub CLI (`gh`)**:
  - Authenticated as `jssyxd`.
  - Proxy configuration: `http://192.168.1.5:7890`.
- **System Memory Architecture**:
  - 12 GB Physical RAM.
  - 12 GB Permanent Swap file (`/swap.img`, swapon active, persistent in `/etc/fstab`).
- **Filesystem Compatibility**:
  - GVFS SMB mount handling: monkey-patched `filelock` mode suppression to handle CIFS/SMB chmod limitations.

---

## 4. Next Steps (Future Swarm)
- Data governance swarm: schema harmonization (standardizing columns to `timestamp`, `open`, `high`, `low`, `close`, `volume`).
- Partition optimization: converting disparate timeframes into unified Iceberg/Parquet partitions.
