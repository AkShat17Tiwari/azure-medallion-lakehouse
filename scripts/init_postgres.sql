-- ==============================================================================
-- PostgreSQL Schema Initialization for Lakehouse Gold Layer
-- ==============================================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS public;

-- Table: Daily Sales Summary
CREATE TABLE IF NOT EXISTS public.gold_daily_sales (
    order_day DATE PRIMARY KEY,
    total_orders BIGINT NOT NULL,
    unique_customers BIGINT NOT NULL,
    total_completed_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    completed_orders BIGINT NOT NULL DEFAULT 0,
    cancelled_orders BIGINT NOT NULL DEFAULT 0,
    _gold_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for date-range analytical queries
CREATE INDEX IF NOT EXISTS idx_gold_daily_sales_order_day 
ON public.gold_daily_sales (order_day DESC);

-- Table: Customer Lifetime Metrics
CREATE TABLE IF NOT EXISTS public.gold_customer_metrics (
    customer_id VARCHAR(64) PRIMARY KEY,
    total_orders BIGINT NOT NULL,
    total_lifetime_spend NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(14, 2),
    latest_order_timestamp TIMESTAMP WITH TIME ZONE,
    _gold_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for customer spend rankings
CREATE INDEX IF NOT EXISTS idx_gold_customer_spend 
ON public.gold_customer_metrics (total_lifetime_spend DESC);

-- View: High-Value Customers (LTV > $200)
CREATE OR REPLACE VIEW public.vw_high_value_customers AS
SELECT 
    customer_id,
    total_orders,
    total_lifetime_spend,
    avg_order_value,
    latest_order_timestamp
FROM public.gold_customer_metrics
WHERE total_lifetime_spend > 200.00
ORDER BY total_lifetime_spend DESC;
