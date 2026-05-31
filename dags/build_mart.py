import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def clean_clickhouse_partition(partition_id):
    ch_conn = BaseHook.get_connection('clickhouse_mart')

    client = Client(
        host=ch_conn.host,
        port=ch_conn.port if ch_conn.port else 9000,
        user=ch_conn.login if ch_conn.login else 'default',
        password=ch_conn.password if ch_conn.password else '',
        database='taxi_mart'
    )

    query = f"ALTER TABLE daily_revenue_by_vendor DROP PARTITION '{partition_id}'"
    print(f"Выполняем очистку: {query}")

    try:
        client.execute(query)
    except Exception as e:
        print(f"Партиция {partition_id} не найдена или ошибка (игнорируем при первом запуске): {e}")

with DAG(
        dag_id='load_dds_to_clickhouse_mart',
        default_args=default_args,
        schedule_interval=None,
        start_date=pendulum.datetime(2025, 12, 1),
        catchup=False,
        tags=['taxi', 'mart', 'spark', 'clickhouse'],
) as dag:
    truncate_ch_partition = PythonOperator(
        task_id='truncate_clickhouse_partition',
        python_callable=clean_clickhouse_partition,
        op_kwargs={'partition_id': '{{ data_interval_start.strftime("%Y%m") }}'},
    )

    build_mart = SparkSubmitOperator(
        task_id='run_spark_build_mart',
        application='/opt/airflow/spark/jobs/build_mart.py',
        conn_id='spark_default',
        name='build_taxi_mart',
        application_args=[
            '{{ data_interval_start.strftime("%Y-%m-%d") }}'
        ],
        packages=(
            'org.postgresql:postgresql:42.7.3,'
            'com.clickhouse:clickhouse-jdbc:0.6.5'
        ),
        executor_memory='1G',
        executor_cores=1,

        conf={
            "spark.gp.url": "jdbc:postgresql://{{ conn.greenplum.host }}:{{ conn.greenplum.port }}/{{ conn.greenplum.schema }}",
            "spark.gp.user": "{{ conn.greenplum.login }}",
            "spark.gp.password": "{{ conn.greenplum.password }}",

            "spark.ch.url": "jdbc:clickhouse://{{ conn.clickhouse_mart.host }}:{{ conn.clickhouse_mart.port }}/taxi_mart",
            "spark.ch.user": "{{ conn.clickhouse_mart.login }}",
            "spark.ch.password": "{{ conn.clickhouse_mart.password }}",
        }
    )

    truncate_ch_partition >> build_mart