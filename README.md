# dataBaseLake · 机构级多资产量化回测湖与数据治理管线

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![Parquet](https://img.shields.io/badge/storage-Apache_Parquet_(Zstd--7)-50C878.svg)]()
[![Governance](https://img.shields.io/badge/governance-Mathematical_Invariants-blueviolet.svg)]()
[![License](https://img.shields.io/badge/license-MIT-informational.svg)]()

> **Centralized Multi-Asset Quantitative Backtest Lake & Governance Pipeline**  
> 专为量化多策略回测（NautilusTrader / VectorBT / Polars / DuckDB）打造的机构级多资产数据中心。统一管理从全网高频历史原始数据湖采集、标准化数据清洗审计，到生产级 Parquet 消费湖的完整生命周期。

---

## 目录 / Table of Contents

- [1. 核心架构与分层设计 (Architecture)](#1-核心架构与分层设计-architecture)
- [2. 新电脑一键快速构建指南 (Quick Start)](#2-新电脑一键快速构建指南-quick-start)
- [3. 大数据源引用矩阵与离线恢复 (Data Sources & Lineage)](#3-大数据源引用矩阵与离线恢复-data-sources--lineage)
- [4. 数据治理契约与五大数学不变量 (Governance Rules)](#4-数据治理契约与五大数学不变量-governance-rules)
- [5. 未来增量更新与自动化入库 (Incremental Updates)](#5-未来增量更新与自动化入库-incremental-updates)
- [6. 高性能读取与回测对接 (Fast Data Loading)](#6-高性能读取与回测对接-fast-data-loading)

---

## 1. 核心架构与分层设计 (Architecture)

本仓库严格遵循工业级量化数仓的分层分级规范，杜绝冗余混乱：

```text
dataBaseLake/
├── bootstrap_lake.py             # 🚀 新电脑一键数据拉取与自检引导工具
├── incremental_update.py         # 🔄 增量行情拉取与五大数学不变量审计引擎
├── data_sources.json             # 🌐 全球多源大数据注册表与国内外镜像路由
├── README.md                     # 📖 本文档
├── .gitignore                    # 🛡️ 智能排除 G 级二进制，精准保留全部代码与契约
│
├── 01_raw_lake/                  # 【L1 原始粗数据湖】全网历史行情不可替代数据源 (~119 GB)
│   ├── huggingface/              # 1m 高频历史 OHLCV (mito0o852)、币安合约全量历史 (123olp)、twelvedata
│   ├── cryptodatadownload/       # Binance, Bitstamp, Bitfinex, KuCoin, Deribit 7大交易所全量历史 CSV
│   ├── github/                   # getdata-finance (美股/外汇/指数) & 经典机器学习量化数据集
│   ├── kaggle/                   # Quandl WIKI 3000+ 美股完整日线与特征数据
│   └── scripts/                  # 官方并发下载器与断点续传脚本
│
├── 02_pipeline_engine/           # 【L2 数据工程与治理管线】清洗、转换、对齐与审计工具链 (~235 MB)
│   └── nautilus_crypto_pipeline/
│       ├── scripts/              # 73 个核心处理脚本（包含 DuckDB 聚合、ClickHouse 迁移同步、Bybit/Binance 审计）
│       ├── catalog/              # NautilusTrader ParquetDataCatalog 本地生成工作区
│       └── validation_reports/   # 连续性与质量审计证明链
│
└── 03_governed_lake/             # 【L3 生产级已治理回测消费湖】高保真、零拷贝 Parquet 数据集 (~2.1 GB)
    ├── v2_parquet_lake/          # 遵循严格 JSON Schema Draft 2020-12 的 Parquet 湖 (Zstd-7)
    │   ├── crypto/               # 30 个主流币种永续合约标准化分片 (15m, 1h, 4h, 1d)
    │   ├── global_futures/       # 原油 CL、黄金 GC、美债 ZN、德指 FDAX、恒指 HSI、日经 NK225
    │   ├── us_equities/          # QQQ, SPY, TQQQ (1m, 15m, 1h, 4h, 1d)
    │   ├── us_futures/           # 标普 ES, 纳指 NQ
    │   ├── manifest/             # manifest.json 注册表与契约 Schema
    │   └── tools/                # db_loader.py (支持 DuckDB/Polars/Pandas 1~2ms 读取)
    ├── nautilus_databank/        # 4227 万根已校验 Bars (大宗商品、加密、股指、外汇、贵金属、国债)
    └── quant_data_inventory/     # 多资产治理清单与外部挂载索引
```

---

## 2. 新电脑一键快速构建指南 (Quick Start)

在任何全新的开发机或云服务器上，仅需 2 步即可完成本数据库的恢复与构建：

### 第一步：克隆仓库架构代码
```bash
git clone https://github.com/jssyxd/dataBaseLake.git
cd dataBaseLake
```

### 第二步：按需一键下载/恢复数据
本仓库内置智能下载引擎 `bootstrap_lake.py`，支持国内外网络自适应（国内自动切换至高带宽镜像 `hf-mirror.com`，支持断点续传）：

```bash
# 1. 查看当前数据湖健康状态与已下载进度
python bootstrap_lake.py --status

# 2. 列出注册表中的所有可用大数据集
python bootstrap_lake.py --list

# 3. 按需下载恢复指定数据集（例如恢复高频 1m OHLCV 数据）
python bootstrap_lake.py --dataset hf_mito0o852_ohlcv1m

# 4. 或者一键拉取整层数据
python bootstrap_lake.py --tier 01_raw_lake

# 5. 海外服务器如需直连官方 HuggingFace，加 --no-mirror
python bootstrap_lake.py --dataset hf_twelvedata_fwm --no-mirror
```

---

## 3. 大数据源引用矩阵与离线恢复 (Data Sources & Lineage)

所有超过 G 级别的大数据均已在此注册，并维护官方原生源与加速镜像：

| 数据集 ID | 资产大类 | 预估体积 | 官方源 / 引用链接 | 恢复指令 |
| :--- | :--- | :---: | :--- | :--- |
| `hf_mito0o852_ohlcv1m` | 全球多资产 1m OHLCV | ~81.7 GB | [HuggingFace mito0o852/OHLCV-1m](https://huggingface.co/datasets/mito0o852/OHLCV-1m) | `python bootstrap_lake.py --dataset hf_mito0o852_ohlcv1m` |
| `hf_123olp_binance_futures` | 币安合约 1m K线与指标 | ~16.1 GB | [HuggingFace 123olp/binance-futures](https://huggingface.co/datasets/123olp/binance-futures-ohlcv-2018-2026) | `python bootstrap_lake.py --dataset hf_123olp_binance_futures` |
| `hf_twelvedata_fwm` | 多模态金融轨迹与K线 | ~47.2 GB | [HuggingFace twelvedata/financial-world-model](https://huggingface.co/datasets/twelvedata/financial-world-model) | `python bootstrap_lake.py --dataset hf_twelvedata_fwm` |
| `hf_omchoksi108_cloud_lake` | 云端多资产数据湖 | ~4.3 GB | [HuggingFace OMCHOKSI108/my-cloud-data-lake](https://huggingface.co/datasets/OMCHOKSI108/my-cloud-data-lake) | `python bootstrap_lake.py --dataset hf_omchoksi108_cloud_lake` |
| `cdd_all_exchanges` | 7大交易所历史 K 线 | ~4.4 GB | [CryptoDataDownload 官方数据站](https://www.cryptodatadownload.com/data/) | `python bootstrap_lake.py --dataset cdd_all_exchanges` |
| `gh_getdata_finance` | 美股/外汇/指数高精 K 线 | ~3.0 GB | [GitHub getdata-finance](https://github.com/getdata-finance) | `python bootstrap_lake.py --dataset gh_getdata_finance` |
| `kaggle_quandl_wiki` | Quandl 3000+ 美股完整日线 | ~1.7 GB | [Kaggle quandl/wiki-prices-us-equities](https://www.kaggle.com/datasets/quandl/wiki-prices-us-equities) | `python bootstrap_lake.py --dataset kaggle_quandl_wiki` |

---

## 4. 数据治理契约与五大数学不变量 (Governance Rules)

所有从 `01_raw_lake` 经过清洗提升至 `03_governed_lake` 的数据必须通过**五大数学不变量验证**：

1. **时间戳严格单调性**：$ts_{i} > ts_{i-1}$，纳秒级 UTC 严格单调递增；
2. **彻底去重**：杜绝时间戳碰撞与重复记录；
3. **K 线几何有效性**：$High \ge \max(Open, Close)$ 且 $Low \le \min(Open, Close)$；
4. **价格正值性**：$Open, High, Low, Close > 0$；
5. **成交量非负性**：$Volume \ge 0$；
6. **真实断点治理**：加密货币 24/7 真实行情中断记录入 `defects.json`，**严禁前向伪造合成插值**；股票期货严格根据法定交易所日历对齐交易时段。

一键运行治理质检：
```bash
python incremental_update.py --audit
```

---

## 5. 未来增量更新与自动化入库 (Incremental Updates)

在日常量化研究或生产盘后，运行增量更新工具即可拉取最新行情并完成合规审计：

```bash
# 增量检查与同步元数据注册表
python incremental_update.py --sync-manifest

# 针对指定标的执行增量拉取与质量质检
python incremental_update.py --symbols BTCUSDT ETHUSDT QQQ SPY
```

---

## 6. 高性能读取与回测对接 (Fast Data Loading)

消费层 `03_governed_lake` 采用统一的 Apache Parquet (Zstd-7 压缩)，支持多进程与多 Agent 零拷贝极速并发访问：

```python
from database.03_governed_lake.v2_parquet_lake.tools.db_loader import load_data

# 单次加载仅需 1~2 ms
df = load_data("BTCUSDT", timeframe="15m", engine="polars")
print(df.head())
```

---

## 7. 许可协议 (License)

本项目采用 MIT License 开源协议，遵循量化金融学术研究与生产使用规范。
