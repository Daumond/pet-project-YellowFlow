import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    print(f"Starting Spark Job for partition date: {target_date}")

    spark = SparkSession.builder \
        .appName("BuildTaxiMart") \
        .getOrCreate()

    gp_url = spark.conf.get("spark.gp.url")
    gp_props = {
        "user": spark.conf.get("spark.gp.user"),
        "password": spark.conf.get("spark.gp.password"),
        "driver": "org.postgresql.Driver"
    }

    fact_query = f"(SELECT * FROM dds.yellow_tripdata WHERE partition_date = '{target_date}') AS fact"
    df_fact = spark.read.jdbc(url=gp_url, table=fact_query, properties=gp_props)
    df_vendor = spark.read.jdbc(url=gp_url, table="dds.dim_vendor", properties=gp_props)
    df_payment = spark.read.jdbc(url=gp_url, table="dds.dim_payment_type", properties=gp_props)

    df_joined = df_fact \
        .join(F.broadcast(df_vendor), on="vendor_id", how="left") \
        .join(F.broadcast(df_payment), on="payment_type", how="left")

    df_mart = df_joined \
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
        .groupBy("pickup_date", "vendor_name", "payment_type_desc") \
        .agg(
        F.count("*").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("tip_amount").alias("avg_tip"),
        F.sum("passenger_count").alias("total_passengers")
    ) \
        .fillna({
        "vendor_name": "Unknown",
        "payment_type_desc": "Unknown",
        "total_revenue": 0.0,
        "avg_tip": 0.0,
        "total_passengers": 0
    })

    print("Writing to ClickHouse via JDBC...")

    ch_url = spark.conf.get("spark.ch.url")


    raw_user = spark.conf.get("spark.ch.user", "default")
    ch_user = raw_user if raw_user and raw_user != "None" else "default"

    raw_password = spark.conf.get("spark.ch.password", "")

    ch_props = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": ch_user,
        "batchsize": "100000"
    }

    if raw_password and raw_password != "None":
        ch_props["password"] = raw_password


    df_mart.write.jdbc(url=ch_url, table="daily_revenue_by_vendor", mode="append", properties=ch_props)

    print("✅ Spark Job completed successfully!")
    spark.stop()