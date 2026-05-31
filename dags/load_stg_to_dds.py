from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dateutil.relativedelta import relativedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def get_next_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return (dt + relativedelta(months=1)).strftime('%Y-%m-%d')



with DAG(
    dag_id='load_stg_to_dds',
    default_args=default_args,
    description='',
    schedule_interval=None,
    start_date=datetime(2025, 12, 1),
    catchup=True,
    max_active_runs=1,
    user_defined_macros={'get_next_month': get_next_month},
    tags=['taxi', 'stg'],
) as dag:

    truncate_partition = SQLExecuteQueryOperator(
        task_id='prepare_and_truncate_greenplum_partition',
        conn_id='greenplum',
        sql="""
               DO $$
               BEGIN
                   IF NOT EXISTS (
                       SELECT 1 FROM pg_partitions 
                       WHERE schemaname = 'dds' AND tablename = 'yellow_tripdata' 
                         AND partitionname = 'p{{ data_interval_start.strftime('%Y_%m') }}'
                   ) THEN
                       EXECUTE 'ALTER TABLE dds.yellow_tripdata ADD PARTITION p{{ data_interval_start.strftime('%Y_%m') }} ' ||
                               'START (DATE ''{{ data_interval_start.strftime('%Y-%m-01') }}'') ' ||
                               'END (DATE ''{{ get_next_month(data_interval_start.strftime("%Y-%m-%d")) }}'')';
                   END IF;
               END $$;

               ALTER TABLE dds.yellow_tripdata TRUNCATE PARTITION p{{ data_interval_start.strftime('%Y_%m') }};
           """,
    )

    load_dds = SQLExecuteQueryOperator(
        task_id="load_dds",
        conn_id="greenplum",
        sql="""
            INSERT INTO dds.yellow_tripdata (
                vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, 
                passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, 
                pu_location_id, do_location_id, payment_type, fare_amount, 
                extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, 
                total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee, 
                source_file, partition_date
            )
            SELECT 
                vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, 
                passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, 
                pu_location_id, do_location_id, payment_type, fare_amount, 
                extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, 
                total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee, 
                source_file, partition_date
            FROM stg.yellow_tripdata
            WHERE partition_date = '{{ data_interval_start.strftime("%Y-%m-01") }}'::DATE;
            """,
    )

    audit_log = SQLExecuteQueryOperator(
        task_id="audit_log",
        conn_id="greenplum",
        sql="""
        INSERT INTO etl.load_log
        (
            dag_id,
            run_id,
            file_name,
            rows_loaded
        )
        SELECT
            '{{ dag.dag_id }}',
            '{{ run_id }}',
            'yellow_tripdata_{{ data_interval_start.strftime("%Y-%m") }}.parquet',
            COUNT(*)
        FROM stg.yellow_tripdata;
        """,
    )

    trigger_mart_load = TriggerDagRunOperator(
        task_id="trigger_mart_load",
        trigger_dag_id="load_dds_to_clickhouse_mart",
        logical_date="{{ logical_date }}",
        wait_for_completion=False,
    )

    (
    truncate_partition
    >> load_dds
    >> audit_log
    >> trigger_mart_load
    )