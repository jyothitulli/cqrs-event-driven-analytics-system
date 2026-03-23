-- Read Database Schema (Denormalized materialized views for analytics)
-- This script runs when the read database container starts

-- 1. Product Sales View - Aggregated sales per product
CREATE TABLE IF NOT EXISTS product_sales_view (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    total_quantity_sold INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Category Metrics View - Aggregated metrics per category
CREATE TABLE IF NOT EXISTS category_metrics_view (
    category_name VARCHAR(100) PRIMARY KEY,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_products_sold INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Customer Lifetime Value View - Customer analytics
CREATE TABLE IF NOT EXISTS customer_ltv_view (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    total_spent DECIMAL(10, 2) NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    average_order_value DECIMAL(10, 2) NOT NULL DEFAULT 0,
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Hourly Sales View - Time-based aggregations
CREATE TABLE IF NOT EXISTS hourly_sales_view (
    hour_timestamp TIMESTAMP PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0,
    total_items_sold INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Event Processing Log - For idempotency (prevents duplicate processing)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_hourly_sales_timestamp ON hourly_sales_view(hour_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_customer_ltv_total_spent ON customer_ltv_view(total_spent DESC);
CREATE INDEX IF NOT EXISTS idx_product_sales_revenue ON product_sales_view(total_revenue DESC);

-- Create a function to automatically update last_updated
CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for last_updated columns
CREATE TRIGGER update_product_sales_last_updated 
    BEFORE UPDATE ON product_sales_view 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_updated_column();

CREATE TRIGGER update_category_metrics_last_updated 
    BEFORE UPDATE ON category_metrics_view 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_updated_column();

CREATE TRIGGER update_customer_ltv_last_updated 
    BEFORE UPDATE ON customer_ltv_view 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_updated_column();

CREATE TRIGGER update_hourly_sales_last_updated 
    BEFORE UPDATE ON hourly_sales_view 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_updated_column();