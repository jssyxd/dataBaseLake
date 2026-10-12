# Cloud Bundle · Multi-Asset OHLCV Dataset

[![Parquet](https://img.shields.io/badge/format-Parquet-50C878)]()
[![Symbols](https://img.shields.io/badge/symbols-24-blue)]()
[![Timeframes](https://img.shields.io/badge/timeframes-8-orange)]()
[![Files](https://img.shields.io/badge/files-14.3k-lightgrey)]()
[![Size](https://img.shields.io/badge/size-~3.5_GB-informational)]()

**EN** · Open, research-oriented **minute-level OHLCV** history for **20 crypto perpetuals (USDT)** and **4 FX/commodity** pairs, stored as **monthly Parquet** shards across **8 bar sizes**.

**中文** · 面向研究与回测开源的 **多标的 OHLCV 数据集**：含 **20 个加密货币 USDT 合约** 与 **4 个外汇/商品** 标的，按 **月份 + 周期** 切分为 **Parquet** 文件，覆盖 **1m～2h** 共 8 种 K 线周期。

> Coverage snapshot: **14,317** Parquet files · **~3.49 GB** uncompressed (as of local bundle scan, 2026-02).

---

## Table of Contents / 目录

- [Why this dataset? / 数据集亮点](#why-this-dataset--数据集亮点)
- [Directory layout / 目录结构](#directory-layout--目录结构)
- [File naming / 文件命名](#file-naming--文件命名)
- [Schema / 字段说明](#schema--字段说明)
- [Symbol coverage / 标的覆盖范围](#symbol-coverage--标的覆盖范围)
- [Data characteristics / 数据特点](#data-characteristics--数据特点)
- [Quick start / 快速上手](#quick-start--快速上手)
- [Caveats / 使用注意](#caveats--使用注意)
- [License / 许可证](#license--许可证)

---

## Why this dataset? / 数据集亮点

| | English | 中文 |
|---|---------|------|
| **Scope** | 24 instruments · 8 timeframes · monthly shards | 24 个标的 · 8 周期 · 按月分片 |
| **Crypto depth** | BTC/ETH from **2017-08**; long tail of altcoins from listing date | 主流币自 **2017-08** 起；山寨币自上线月起 |
| **Traditional** | EUR/GBP/Gold/Oil from **2020-01** (FX session-aware bars) | 外汇/黄金/原油自 **2020-01** 起（仅含可交易时段） |
| **Format** | Columnar Parquet, `DatetimeIndex`, ready for pandas / Polars / DuckDB | Parquet 列存，时间索引，可直接用 pandas / Polars / DuckDB |
| **Use cases** | Backtesting, factor research, ML feature pipelines, cross-asset studies | 回测、因子研究、机器学习特征工程、跨资产研究 |

---

## Directory layout / 目录结构

```
cloud_bundle/
├── coinraw_data/                    # Crypto (USDT-margined perpetuals)
│   ├── BTCUSDTraw_data/
│   │   └── ohlcv/
│   │       ├── 1m/                  # 1-minute bars
│   │       ├── 2m/
│   │       ├── 5m/
│   │       ├── 10m/
│   │       ├── 15m/
│   │       ├── 30m/
│   │       ├── 1h/
│   │       └── 2h/
│   ├── ETHUSDTraw_data/
│   └── …                            # 18 more altcoin folders
├── eurusdraw_data/ohlcv/{1m,2m,…,2h}/
├── gbpusdraw_data/ohlcv/…
├── usousdraw_data/ohlcv/…           # US Oil (USOUSD)
└── xauusdraw_data/ohlcv/…           # Gold (XAUUSD)
```

**Timeframes (all symbols):** `1m`, `2m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h`

---

## File naming / 文件命名

| Asset class | Pattern | Example |
|-------------|---------|---------|
| Crypto | `{SYMBOL}_{TF}_ohlcv_{YYYY-MM}.parquet` | `BTCUSDT_1m_ohlcv_2020-01.parquet` |
| FX / Commodity | `{symbol}_{TF}_ohlcv_{YYYY-MM}.parquet` | `eurusd_1m_ohlcv_2020-01.parquet` |

Each file holds **one calendar month** of bars for that symbol and timeframe.

---

## Schema / 字段说明

| Column | Type | Description EN | 说明（中文） |
|--------|------|----------------|--------------|
| *(index)* | `datetime64[ns]` | Bar open time (naive; **UTC** for 24/7 crypto) | K 线开盘时间（无时区；加密货币按 **UTC** 理解） |
| `open` | `float64` | Open price | 开盘价 |
| `high` | `float64` | High price | 最高价 |
| `low` | `float64` | Low price | 最低价 |
| `close` | `float64` | Close price | 收盘价 |
| `volume` | `float64` / `int64` | Traded volume in the bar (crypto: float; FX: int tick/lot style) | 成交量（加密货币多为 float；外汇多为 int） |

---

## Symbol coverage / 标的覆盖范围

Ranges below are derived from **on-disk monthly files** (first month → last month).  
**Crypto `1m`** is the canonical series for bar count; **`5m`–`2h`** may include **one extra month** (e.g. `2026-02`) vs `1m` when the bundle was exported in early February 2026.

下表为仓库内 **实际文件** 的起止月份；加密货币以 **`1m`** 为准，较高周期可能多 **最近一个不完整月份**。

### FX & Commodities / 外汇与商品

| Symbol | Folder | Range (YYYY-MM) | EN notes | 中文说明 |
|--------|--------|-----------------|----------|----------|
| **EURUSD** | `eurusdraw_data/` | **2020-01 → 2026-02** | Major FX pair; ~74 monthly files per TF | 欧元美元；各周期约 74 个月文件 |
| **GBPUSD** | `gbpusdraw_data/` | **2020-01 → 2026-02** | Major FX pair | 英镑美元 |
| **XAUUSD** | `xauusdraw_data/` | **2020-01 → 2026-02** | Spot gold vs USD | 现货黄金 |
| **USOUSD** | `usousdraw_data/` | **2020-01 → 2026-02** | US crude oil vs USD | 美国原油 |

### Crypto (USDT perpetuals) / 加密货币（USDT 永续）

| Symbol | Folder | Range `1m` (YYYY-MM) | Range higher TF\* | EN notes | 中文说明 |
|--------|--------|----------------------|---------------------|----------|----------|
| **BTCUSDT** | `coinraw_data/BTCUSDTraw_data/` | **2017-08 → 2026-01** | → 2026-02 | Longest history in bundle | Bundle 内最长历史 |
| **ETHUSDT** | `coinraw_data/ETHUSDTraw_data/` | **2017-08 → 2026-01** | → 2026-02 | Same era as BTC | 与 BTC 同期 |
| **BNBUSDT** | `coinraw_data/BNBUSDTraw_data/` | **2017-11 → 2026-01** | → 2026-02 | Early exchange-era data | 早期上所数据 |
| **LTCUSDT** | `coinraw_data/LTCUSDTraw_data/` | **2017-12 → 2026-01** | → 2026-02 | | |
| **TRXUSDT** | `coinraw_data/TRXUSDTraw_data/` | **2018-06 → 2026-01** | → 2026-02 | | |
| **ADAUSDT** | `coinraw_data/ADAUSDTraw_data/` | **2018-04 → 2026-01** | → 2026-02 | | |
| **XRPUSDT** | `coinraw_data/XRPUSDTraw_data/` | **2018-05 → 2026-01** | → 2026-02 | | |
| **LINKUSDT** | `coinraw_data/LINKUSDTraw_data/` | **2019-01 → 2026-01** | → 2026-02 | | |
| **MATICUSDT** | `coinraw_data/MATICUSDTraw_data/` | **2019-04 → 2024-09** | same | **Stops mid-month** (last bars ≈ 2024-09-10); no later months | **已停更**：末月不完整（约至 2024-09-10），无后续月份 |
| **BCHUSDT** | `coinraw_data/BCHUSDTraw_data/` | **2019-11 → 2026-01** | → 2026-02 | | |
| **DOGEUSDT** | `coinraw_data/DOGEUSDTraw_data/` | **2019-07 → 2026-01** | → 2026-02 | | |
| **DOTUSDT** | `coinraw_data/DOTUSDTraw_data/` | **2020-08 → 2026-01** | → 2026-02 | | |
| **SOLUSDT** | `coinraw_data/SOLUSDTraw_data/` | **2020-08 → 2026-01** | → 2026-02 | | |
| **UNIUSDT** | `coinraw_data/UNIUSDTraw_data/` | **2020-09 → 2026-01** | → 2026-02 | | |
| **AVAXUSDT** | `coinraw_data/AVAXUSDTraw_data/` | **2020-09 → 2026-01** | → 2026-02 | | |
| **NEARUSDT** | `coinraw_data/NEARUSDTraw_data/` | **2020-10 → 2026-01** | → 2026-02 | | |
| **SHIBUSDT** | `coinraw_data/SHIBUSDTraw_data/` | **2021-05 → 2026-01** | → 2026-02 | Meme-era listing | Meme 币世代 |
| **ICPUSDT** | `coinraw_data/ICPUSDTraw_data/` | **2021-05 → 2026-01** | → 2026-02 | | |
| **PEPEUSDT** | `coinraw_data/PEPEUSDTraw_data/` | **2023-05 → 2026-01** | → 2026-02 | Short history (~33 months on 1m) | 历史较短（1m 约 33 个月） |
| **RENDERUSDT** | `coinraw_data/RENDERUSDTraw_data/` | **2024-07 → 2026-01** | → 2026-02 | Newest listing in bundle | bundle 内最新标的 |

\*Higher TF = `2m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h` — typically **+1 month** at the end vs `1m` for active symbols.

---

## Data characteristics / 数据特点

### 1. Storage model / 存储模型

- **Sharding:** one Parquet file per **symbol × timeframe × month** — easy parallel reads and incremental updates.
- **Compression:** Parquet columnar encoding; friendly to object storage (S3/OSS) and local SSD scans.
- **分片方式：** 按 **标的 × 周期 × 自然月** 存储，便于并行读取与增量更新。

### 2. Crypto (24/7) / 加密货币（7×24）

- **Calendar:** continuous UTC-style timestamps; e.g. BTC `1m` January files contain **44,640** bars (full month).
- **Volume:** `float64` base-asset or contract volume (exchange-native).
- **Higher timeframes:** pre-aggregated bars; **not guaranteed** to equal `resample()` from `1m` tick-for-tick (exchange bar rules differ).
- **7×24 连续报价**；`1m` 整月 K 线数量完整；高周期为预聚合，与自行 `resample` 可能有细微差异。

### 3. FX & commodities (session-based) / 外汇与商品（交易时段）

- **Sparse 1m grids:** fewer bars per month than `days × 1440` — only **market-open** minutes are present (e.g. EURUSD ~31k bars vs ~44k for a 31-day crypto month).
- **Weekends / holidays:** no synthetic fill; gaps are real non-trading time.
- **Volume:** integer type on FX samples (tick/volume count style).
- **仅含可交易分钟**；周末/节假日无填充；成交量为整数型计数风格。

### 4. Cross-symbol consistency / 跨标的一致性

- Same **8 timeframes** and **OHLCV column layout** across all instruments.
- **Start dates differ** by listing / data availability (see table above).
- **MATICUSDT** is the main exception: history **ends 2024-09** with a **partial last month**.

### 5. Known bundle export quirks / 导出批次说明

| Topic | EN | 中文 |
|-------|----|------|
| **End month skew** | Crypto `1m` often ends **2026-01** while `5m+` may include **2026-02** | 加密货币 `1m` 常止于 2026-01，较高周期可能含 2026-02 |
| **Partial months** | First/last month in a symbol’s life may be incomplete | 标的上市/下市当月可能不完整 |
| **Timezone** | Index is timezone-naive; treat crypto as **UTC** | 索引无时区；加密货币建议按 **UTC** 处理 |
| **No order book** | OHLCV only — no L2, trades, or funding rates | 仅 OHLCV，无订单簿/逐笔/资金费率 |

---

## Quick start / 快速上手

### Requirements / 依赖

```bash
pip install pandas pyarrow
# optional: polars duckdb
```

### Load one month / 读取单月

```python
import pandas as pd
from pathlib import Path

ROOT = Path("cloud_bundle")  # adjust to your clone path

# Crypto example
path = ROOT / "coinraw_data/BTCUSDTraw_data/ohlcv/1m/BTCUSDT_1m_ohlcv_2024-01.parquet"
btc = pd.read_parquet(path)
print(btc.head())

# FX example
path = ROOT / "eurusdraw_data/ohlcv/1h/eurusd_1h_ohlcv_2024-01.parquet"
eur = pd.read_parquet(path)
```

### Stitch a year / 拼接一年

```python
import pandas as pd

def load_year(root: Path, pattern: str, year: int) -> pd.DataFrame:
    files = sorted(root.glob(f"{pattern}_{year}-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No files for {year} under {root}")
    return pd.concat((pd.read_parquet(f) for f in files), axis=0).sort_index()

btc_2024 = load_year(
    ROOT / "coinraw_data/BTCUSDTraw_data/ohlcv/1m",
    "BTCUSDT_1m_ohlcv",
    2024,
)
```

### DuckDB (SQL over files) / 用 DuckDB 扫目录

```sql
INSTALL parquet;
SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
FROM read_parquet('coinraw_data/BTCUSDTraw_data/ohlcv/1m/*.parquet');
```

---

## Caveats / 使用注意

1. **Not investment advice.** For research and education only.  
   **非投资建议**，仅供研究与学习。

2. **Survivorship & listings:** Altcoin start dates reflect **when this feed began**, not necessarily global first trade.  
   山寨币起始日为 **本数据源收录时间**，未必等于全球首笔成交。

3. **Corporate actions / symbol changes:** e.g. MATIC → POL rebranding is **not** merged here.  
   不含拆股、换标等公司行为处理（如 MATIC  rebranding）。

4. **Backtest realism:** Account for fees, slippage, session rules, and incomplete months.  
   回测请自行加入手续费、滑点、交易时段与不完整月份处理。

5. **Verify before publish:** Re-scan file counts after adding/removing months.  
   上传前建议重新统计文件数与起止月份。


---

## Contributing / 贡献

Issues and PRs welcome for:

- Coverage corrections (date ranges, missing months)
- Example notebooks (factor calc, train/val split)
- English/Chinese doc improvements

欢迎提交：覆盖范围修正、示例 Notebook、文档改进。

---

## Citation / 引用

If you use this dataset in academic or open work, please cite the repository URL and access date:

如在论文或开源项目中使用，请引用本仓库 URL 与访问日期：