CREATE DATABASE IF NOT EXISTS taxi_mart;

CREATE TABLE IF NOT EXISTS taxi_mart.daily_revenue_by_vendor (
    pickup_date Date,
    vendor_name String,
    payment_type_desc String,
    total_trips UInt64,
    total_revenue Float64,
    avg_tip Float64,
    total_passengers UInt64,
    load_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_date, vendor_name, payment_type_desc);