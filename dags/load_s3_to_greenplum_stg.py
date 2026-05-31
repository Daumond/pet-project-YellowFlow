from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dateutil.relativedelta import relativedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def get_next_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return (dt + relativedelta(months=1)).strftime('%Y-%m-%d')



with DAG(
    dag_id='load_s3_to_greenplum_stg',
    default_args=default_args,
    description='Idempotent data load from MinIO to Greenplum via PXF with dynamic partitions',
    schedule_interval='@monthly',
    start_date=datetime(2025, 12, 1),
    catchup=True,
    max_active_runs=1,
    user_defined_macros={'get_next_month': get_next_month},
    tags=['taxi', 'stg'],
) as dag:
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="nyc-taxi-snappy",
        bucket_key="yellow_tripdata_{{ data_interval_start.strftime('%Y-%m') }}.parquet",
        aws_conn_id="minio",
        mode="reschedule",
        poke_interval=60
    )


    recreate_ext_table = SQLExecuteQueryOperator(
        task_id="recreate_pxf_external_table",
        conn_id="greenplum",
        sql="""
        DROP EXTERNAL TABLE IF EXISTS stg.ext_yellow_tripdata;

        CREATE EXTERNAL TABLE stg.ext_yellow_tripdata (
            "VendorID" INTEGER,
            tpep_pickup_datetime BIGINT,
            tpep_dropoff_datetime BIGINT,
            passenger_count BIGINT,
            trip_distance DOUBLE PRECISION,
            "RatecodeID" BIGINT,
            store_and_fwd_flag TEXT,
            "PULocationID" INTEGER,
            "DOLocationID" INTEGER,
            payment_type BIGINT,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            "Airport_fee" DOUBLE PRECISION,
            cbd_congestion_fee DOUBLE PRECISION
        )
        LOCATION (
            'pxf://nyc-taxi-snappy/yellow_tripdata_{{ data_interval_start.strftime("%Y-%m") }}.parquet?PROFILE=s3:parquet&SERVER=minio'
        )
        FORMAT 'CUSTOM'
        (FORMATTER='pxfwritable_import');
        """,
    )

    truncate_stg = SQLExecuteQueryOperator(
        task_id="truncate_stg",
        conn_id="greenplum",
        sql="""
        DELETE FROM stg.yellow_tripdata 
        WHERE partition_date = '{{ data_interval_start.strftime("%Y-%m-01") }}'::DATE;
        """,
    )

    load_stg = SQLExecuteQueryOperator(
        task_id="load_stg",
        conn_id="greenplum",
        sql="""
        INSERT INTO stg.yellow_tripdata
        (
            vendor_id,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            ratecode_id,
            store_and_fwd_flag,
            pu_location_id,
            do_location_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee,
            source_file,
            partition_date
        )
        SELECT
            "VendorID",
            to_timestamp(tpep_pickup_datetime / 1000000.0)::timestamp,
            to_timestamp(tpep_dropoff_datetime / 1000000.0)::timestamp,
            passenger_count,
            trip_distance,
            "RatecodeID",
            store_and_fwd_flag,
            "PULocationID",
            "DOLocationID",
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            "Airport_fee",
            cbd_congestion_fee,
            'yellow_tripdata_{{ data_interval_start.strftime("%Y-%m") }}.parquet',
            '{{ data_interval_start.strftime("%Y-%m-01") }}'::DATE
        FROM stg.ext_yellow_tripdata;
        """,
    )

    dq_check = SQLExecuteQueryOperator(
        task_id="dq_check",
        conn_id="greenplum",
        sql="""
        DO $$
        DECLARE
            v_cnt BIGINT;
        BEGIN
            SELECT COUNT(*)
            INTO v_cnt
            FROM stg.yellow_tripdata;

            IF v_cnt = 0 THEN
                RAISE EXCEPTION 'STG is empty';
            END IF;
        END $$;
        """,
    )

    trigger_dds_load = TriggerDagRunOperator(
        task_id="trigger_dds_load",
        trigger_dag_id="load_stg_to_dds",
        logical_date="{{ logical_date }}",
        wait_for_completion=False,
    )


    (
        wait_for_file
        >> recreate_ext_table
        >> truncate_stg
        >> load_stg
        >> dq_check
        >> trigger_dds_load
    )
