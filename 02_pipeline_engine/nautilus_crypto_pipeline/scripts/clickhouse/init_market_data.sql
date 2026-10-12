-- ============================================================================
-- ClickHouse TSDB Market Data Schema & Fast Load Definitions
-- Dual-tier Quantitative Architecture: NautilusTrader Parquet Catalog <-> ClickHouse
-- File: init_market_data.sql
-- ============================================================================

CREATE DATABASE IF NOT EXISTS market_data;

-- ----------------------------------------------------------------------------
-- 1. Bars 1-Minute Table (ReplacingMergeTree for deduplication)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_data.bars_1m (
    instrument_id LowCardinality(String),
    bar_type LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    ts_init DateTime64(9, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64 DEFAULT 0,
    trades_count UInt64 DEFAULT 0,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 2. Multi-Timeframe Bars (15m, 1h, 4h, 1D)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_data.bars_multi_tf (
    instrument_id LowCardinality(String),
    timeframe LowCardinality(String), -- '15m', '1h', '4h', '1d'
    bar_type LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    ts_init DateTime64(9, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64 DEFAULT 0,
    trades_count UInt64 DEFAULT 0,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, timeframe, ts_event)
ORDER BY (instrument_id, timeframe, ts_event)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 3. Order Book Deltas (Level-2 Incremental Depth)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_data.order_book_deltas (
    instrument_id LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    ts_init DateTime64(9, 'UTC'),
    action Enum8('ADD' = 1, 'MODIFY' = 2, 'DELETE' = 3, 'CLEAR' = 4),
    side Enum8('BUY' = 1, 'SELL' = 2),
    price Float64,
    size Float64,
    order_id UInt64 DEFAULT 0,
    seq_id UInt64 DEFAULT 0,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree(seq_id)
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event, seq_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 4. Trades & Quote Ticks (BBO)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_data.trade_ticks (
    instrument_id LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    ts_init DateTime64(9, 'UTC'),
    price Float64,
    size Float64,
    aggressor_side Enum8('BUY' = 1, 'SELL' = 2, 'NO_AGGRESSOR' = 0),
    trade_id String,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event, trade_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 5. Derivatives Funding Rates & Open Interest
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_data.funding_rates (
    instrument_id LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    funding_rate Float64,
    predicted_rate Float64 DEFAULT 0,
    mark_price Float64 DEFAULT 0,
    index_price Float64 DEFAULT 0,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.open_interest (
    instrument_id LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    open_interest Float64,
    open_interest_usd Float64,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event)
SETTINGS index_granularity = 8192;
