CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS etl;


DROP TABLE IF EXISTS dds.yellow_tripdata;

CREATE TABLE dds.yellow_tripdata (
    vendor_id INTEGER,

    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,

    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,

    ratecode_id BIGINT,

    store_and_fwd_flag TEXT,

    pu_location_id INTEGER,
    do_location_id INTEGER,

    payment_type BIGINT,

    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,

    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,

    congestion_surcharge DOUBLE PRECISION,

    airport_fee DOUBLE PRECISION,

    cbd_congestion_fee DOUBLE PRECISION,

    source_file TEXT,
    load_date TIMESTAMP DEFAULT clock_timestamp(),

    partition_date DATE
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(partition_date)
(
    PARTITION p2026_01 START (DATE '2026-01-01') END (DATE '2026-02-01'),
    PARTITION p2026_02 START (DATE '2026-02-01') END (DATE '2026-03-01')
  );


CREATE TABLE stg.yellow_tripdata
(
    LIKE dds.yellow_tripdata
)
DISTRIBUTED RANDOMLY;

-- 1. Справочник вендоров
CREATE TABLE dds.dim_vendor (
    vendor_id INTEGER,
    vendor_name TEXT
) DISTRIBUTED REPLICATED;

INSERT INTO dds.dim_vendor (vendor_id, vendor_name) VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'Curb Mobility, LLC'),
(6, 'Myle Technologies Inc'),
(7, 'Helix');

-- 2. Справочник типов оплаты
CREATE TABLE dds.dim_payment_type (
    payment_type BIGINT,
    payment_type_desc TEXT
) DISTRIBUTED REPLICATED;

INSERT INTO dds.dim_payment_type (payment_type, payment_type_desc) VALUES
(0, 'Flex Fare trip')
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');





CREATE TABLE etl.load_log (
    dag_id TEXT,
    run_id TEXT,
    file_name TEXT,
    rows_loaded BIGINT,
    load_ts TIMESTAMP DEFAULT clock_timestamp()
)
DISTRIBUTED RANDOMLY;



