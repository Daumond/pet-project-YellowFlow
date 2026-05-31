import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()

    src = f"s3a://nyc-taxi/yellow_tripdata_{args.ds}.parquet"

    dst_final_file = f"s3a://nyc-taxi-snappy/yellow_tripdata_{args.ds}.parquet"
    dst_tmp_dir = f"s3a://nyc-taxi-snappy/yellow_tripdata_{args.ds}_tmp"

    spark = (
        SparkSession.builder
        .appName(f"parquet-recompress-{args.ds}")
        .getOrCreate()
    )

    df = spark.read.parquet(src)

    df = (
        df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
    )

    total_count = df.count()
    df_valid = df.filter(
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    )

    valid_count = df_valid.count()
    print(f"Validation Stats for {args.ds}: Total rows = {total_count}, Valid rows = {valid_count}")

    if valid_count == 0 and total_count > 0:
        print("CRITICAL ERROR: 0 rows passed validation. Dropping execution.", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    print(f"Writing temporary data to {dst_tmp_dir}...")
    (
        df_valid.coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(dst_tmp_dir)
    )

    print("Extracting and renaming part file to final destination...")
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        Path = sc._jvm.org.apache.hadoop.fs.Path

        tmp_path = Path(dst_tmp_dir)
        fs = tmp_path.getFileSystem(conf)

        file_statuses = fs.listStatus(tmp_path)
        part_file_path = None

        for status in file_statuses:
            file_name = status.getPath().getName()
            if file_name.startswith("part-") and file_name.endswith(".parquet"):
                part_file_path = status.getPath()
                break

        if part_file_path:
            final_path = Path(dst_final_file)

            if fs.exists(final_path):
                fs.delete(final_path, True)

            fs.rename(part_file_path, final_path)
            print(f"SUCCESS: File successfully moved to {dst_final_file}")

            fs.delete(tmp_path, True)
        else:
            print("ERROR: Spark part-file not found in temporary directory!", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"ERROR during renaming process: {str(e)}", file=sys.stderr)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()