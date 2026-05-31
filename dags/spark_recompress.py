from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        dag_id='spark_recompress_and_validate_dag',
        default_args=default_args,
        description='Triggers PySpark job to validate and recompress NYC taxi Parquet files',
        schedule_interval='@monthly',
        start_date=datetime(2025, 12, 1),
        catchup=True,
        max_active_runs=1,
        tags=['taxi', 'spark', 'stg'],
) as dag:
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="nyc-taxi",
        bucket_key="yellow_tripdata_{{ data_interval_start.strftime('%Y-%m') }}.parquet",
        aws_conn_id="minio",
        mode="reschedule",
        poke_interval=60
    )


    run_spark_recompress = SparkSubmitOperator(
        task_id="run_spark_recompress_job",
        conn_id="spark_default",

        application="/opt/airflow/spark/jobs/recompress.py",

        application_args=[
            "--ds", "{{ data_interval_start.strftime('%Y-%m') }}"
        ],

        packages="org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6",

        conf={
            "spark.hadoop.fs.s3a.endpoint": "{{ conn.minio.extra_dejson.endpoint_url }}",
            "spark.hadoop.fs.s3a.access.key": "{{ conn.minio.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.minio.password }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.timeout": "200000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "30000",
            "spark.hadoop.fs.s3a.threads.keepalivetime": "60",
            "spark.hadoop.fs.s3a.connection.ttl": "300000",
            "spark.hadoop.fs.s3a.multipart.purge.age": "86400",
            "spark.hadoop.fs.s3a.assumed.role.session.duration": "3600",
        },
    )

    wait_for_file >> run_spark_recompress