-- ==============================================================================
-- PostgreSQL Serving Layer DDL: Medallion Architecture Gold Data Marts
-- ==============================================================================
-- Target Database: lakehouse_gold
-- Schema: public
-- Description: Optimized relational tables for low-latency BI dashboards & APIs.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS public;

-- ------------------------------------------------------------------------------
-- 1. Table: gold_daily_orders_summary (E-Commerce Daily Order Rollups)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gold_daily_orders_summary (
    order_date DATE NOT NULL,
    order_status VARCHAR(32) NOT NULL,
    total_orders BIGINT NOT NULL DEFAULT 0,
    unique_customers BIGINT NOT NULL DEFAULT 0,
    avg_delivery_days NUMERIC(8, 2),
    delivered_orders BIGINT NOT NULL DEFAULT 0,
    cancelled_orders BIGINT NOT NULL DEFAULT 0,
    report_year INT NOT NULL,
    report_month INT NOT NULL,
    _gold_calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _postgres_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_gold_daily_orders PRIMARY KEY (order_date, order_status)
);

-- Indexes for rapid time-series filtering and dashboard slicing
CREATE INDEX IF NOT EXISTS idx_gold_daily_orders_date 
    ON public.gold_daily_orders_summary (order_date DESC);

CREATE INDEX IF NOT EXISTS idx_gold_daily_orders_year_month 
    ON public.gold_daily_orders_summary (report_year DESC, report_month DESC);

CREATE INDEX IF NOT EXISTS idx_gold_daily_orders_status 
    ON public.gold_daily_orders_summary (order_status);


-- ------------------------------------------------------------------------------
-- 2. Table: gold_daily_zone_metrics (NYC Taxi Multi-Dimensional Zone Rollups)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gold_daily_zone_metrics (
    pickup_date DATE NOT NULL,
    pulocationid INT NOT NULL,
    payment_type INT NOT NULL,
    total_trips BIGINT NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_trip_distance NUMERIC(8, 2) NOT NULL DEFAULT 0.00,
    avg_fare_per_trip NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    avg_tip_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    report_year INT NOT NULL,
    report_month INT NOT NULL,
    _gold_calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _postgres_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_gold_daily_zone PRIMARY KEY (pickup_date, pulocationid, payment_type)
);

-- Indexes for zone lookups and revenue ranking
CREATE INDEX IF NOT EXISTS idx_gold_zone_metrics_date 
    ON public.gold_daily_zone_metrics (pickup_date DESC);

CREATE INDEX IF NOT EXISTS idx_gold_zone_metrics_zone_rev 
    ON public.gold_daily_zone_metrics (pulocationid, total_revenue DESC);


-- ------------------------------------------------------------------------------
-- 3. Table: gold_customer_metrics (Customer Lifetime Value Mart)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gold_customer_metrics (
    customer_id VARCHAR(64) NOT NULL PRIMARY KEY,
    total_orders BIGINT NOT NULL DEFAULT 0,
    total_lifetime_spend NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(10, 2),
    latest_order_timestamp TIMESTAMPTZ,
    _gold_calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _postgres_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_customer_spend 
    ON public.gold_customer_metrics (total_lifetime_spend DESC);


-- ------------------------------------------------------------------------------
-- 4. Analytical Views for BI Tool Acceleration
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_monthly_order_trends AS
SELECT 
    report_year,
    report_month,
    SUM(total_orders) AS monthly_orders,
    SUM(delivered_orders) AS monthly_delivered,
    SUM(cancelled_orders) AS monthly_cancelled,
    ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_time_days
FROM public.gold_daily_orders_summary
GROUP BY report_year, report_month
ORDER BY report_year DESC, report_month DESC;

CREATE OR REPLACE VIEW public.vw_top_revenue_pickup_zones AS
SELECT 
    pulocationid,
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_zone_revenue,
    ROUND(AVG(avg_fare_per_trip), 2) AS avg_zone_fare
FROM public.gold_daily_zone_metrics
GROUP BY pulocationid
ORDER BY total_zone_revenue DESC;
