# Centralized Quant Data Lake - Time Index Governance Specification
## 数据湖时钟索引口径对齐规范：杜绝 Left-Open 与 Right-Close 对齐陷阱

### 1. 行业对齐陷阱背景 (The Alignment Trap)
在量化多资产、高频、宏观时序与事件驱动回测中，不同系统对 K 线（Bar）时间戳的定义存在根本性口径差异：
1. **静态数据湖（Data Lake / Parquet / CCXT / DuckDB）**：
   - 习惯使用 **柱线左开盘时刻（Left-Open Timestamp）** 作为分区键和时间主键。
   - 例如：`00:00:00` 表示该柱线采集的时间窗口为 `[00:00:00, 00:15:00)`。
2. **事件驱动实盘引擎（NautilusTrader, Async Event Nodes, WebSocket Stream）**：
   - 依据因果律（Causality），一根 15 分钟 K 线在未完全走完前其最终收盘价 $Close$ 和成交量 $Volume$ 不存在。
   - 交易所仅在 `00:15:00.000` 瞬间封闭当前 K 线并推送 `on_bar` 结算事件。
   - 因此实盘引擎严格使用 **柱线右收盘完成时刻（Right-Close Timestamp）**（即 `00:15:00`）触发策略执行。

---

### 2. 对齐失准带来的灾难性后果 (Catastrophic Pitfalls)
- **前视偏差 (Lookahead Bias)**：如果将标记为 `00:00:00` 的柱线直接喂入回测引擎，引擎会在模拟时钟 `00:00:00` 提前获取 `00:15:00` 的收盘价进行决策，导致虚假过拟合。
- **实盘对齐时间错位 (Timestamp Mismatch)**：在将实盘交易记录与离线回测对齐时，如果不做时钟平移，同一笔由于 `12:45~13:00` 行情触发的交易，在回测表里显示为 `12:45`，在实盘日志中显示为 `13:00`，造成看似“差 15 分钟乃至数小时”的伪差异。

---

### 3. 数据湖治理标准与转换规约 (Governance Standard)

为保障与 NautilusTrader 及所有事件驱动回测引擎的 100% 物理保真，**dataBaseLake** 建立以下标准契约：

#### 3.1 时间戳数学换算公式
对任意时间粒度 $\Delta T_{\text{bar}}$（如 1m, 15m, 1h, 4h, 1d）：
$$T_{\text{event\_close}} = T_{\text{parquet\_left\_open}} + \Delta T_{\text{bar}}$$

#### 3.2 Nautilus Ingestion 标准管道代码规范
在通过 `BarDataWranglerV2` 将 Parquet 导入 `ParquetDataCatalog` 时，必须强制执行时钟平移：

```python
import pandas as pd
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2

# 1. 读取数据湖 Left-Open Parquet
df = pd.read_parquet(parquet_path)

# 2. 强制时钟平移修正：平移一个周期至 Right-Close 完备时刻
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True) + pd.Timedelta(minutes=15)

# 3. 排序并写入 Nautilus 原生 Catalog
df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
wrangler = BarDataWranglerV2(bar_type="BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL", price_precision=2, size_precision=4)
bars = wrangler.from_pandas(df)
catalog.write_data(bars)
```

---
*规范维护：dataBaseLake Quantitative Governance Committee*  
*最新修订：2026-10-16*
