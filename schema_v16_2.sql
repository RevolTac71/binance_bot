-- ==========================================
-- [V16.2 ML Data Pipeline DDL Script]
-- 용도: Supabase SQL Editor 실행용
-- ==========================================

-- 1. MarketSnapshot 테이블 생성
CREATE TABLE IF NOT EXISTS public.market_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    symbol VARCHAR(20),
    
    rsi DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,
    adx DOUBLE PRECISION,
    atr_14 DOUBLE PRECISION,
    atr_200 DOUBLE PRECISION,
    bb_width DOUBLE PRECISION,
    
    ema_1h_dist DOUBLE PRECISION,
    ema_15m_dist DOUBLE PRECISION,
    
    cvd_5m_sum DOUBLE PRECISION,
    cvd_15m_sum DOUBLE PRECISION,
    cvd_delta_slope DOUBLE PRECISION,
    bid_ask_imbalance DOUBLE PRECISION,
    
    funding_rate_match INTEGER
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_timestamp ON public.market_snapshots (timestamp);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_symbol ON public.market_snapshots (symbol);


-- 2. TradeLog 테이블 생성
CREATE TABLE IF NOT EXISTS public.trade_logs (
    trade_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    direction VARCHAR(10),
    qty DOUBLE PRECISION,
    
    entry_time TIMESTAMP WITHOUT TIME ZONE,
    target_price DOUBLE PRECISION,
    execution_price DOUBLE PRECISION,
    slippage DOUBLE PRECISION,
    entry_reason TEXT,
    execution_time_ms INTEGER,
    
    exit_time TIMESTAMP WITHOUT TIME ZONE,
    exit_price DOUBLE PRECISION,
    exit_reason TEXT,
    realized_pnl DOUBLE PRECISION,
    roi_pct DOUBLE PRECISION,
    
    mfe DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    ret_5m DOUBLE PRECISION,
    ret_15m DOUBLE PRECISION,
    ret_30m DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_trade_logs_symbol ON public.trade_logs (symbol);
CREATE INDEX IF NOT EXISTS idx_trade_logs_entry_time ON public.trade_logs (entry_time);


-- 3. OrderEvent 테이블 생성
CREATE TABLE IF NOT EXISTS public.order_events (
    event_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    symbol VARCHAR(20),
    order_type VARCHAR(20),
    event_type VARCHAR(50),
    price DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    attempt_count INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_order_events_timestamp ON public.order_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_order_events_symbol ON public.order_events (symbol);

-- 권한 부여 (필요 시)
-- GRANT ALL PRIVILEGES ON TABLE public.market_snapshots TO postgres, anon, authenticated, service_role;
-- GRANT ALL PRIVILEGES ON TABLE public.trade_logs TO postgres, anon, authenticated, service_role;
-- GRANT ALL PRIVILEGES ON TABLE public.order_events TO postgres, anon, authenticated, service_role;
