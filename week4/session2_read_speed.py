import argparse
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    TimestampNTZType,
    DoubleType,
    StringType,
)

from common.spark_session import get_spark

def benchmark_reads(orders, format_name, runs):
    count_times = []
    sum_times = []

    for run in range(runs):
        start = perf_counter()
        row_count = orders.count()
        count_elapsed = perf_counter() - start
        count_times.append(count_elapsed)

        start = perf_counter()
        total_amount = (
            orders
            .agg(F.sum("amount").alias("total_amount"))
            .collect()[0]["total_amount"]
        )
        sum_elapsed = perf_counter() - start
        sum_times.append(sum_elapsed)

        print(
            f"{format_name} run {run + 1}: "
            f"count={count_elapsed:.3f}s, "
            f"sum={sum_elapsed:.3f}s"
        )

    print(f"{format_name} row count: {row_count:,}")
    print(f"{format_name} total amount: {total_amount}")
    print(
        f"{format_name} count median: "
        f"{median(count_times[1:]):.3f}s"
    )
    print(
        f"{format_name} sum median: "
        f"{median(sum_times[1:]):.3f}s"
    )

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare reading speeds across file formats"
    )

    parser.add_argument(
        "--formats-dir",
        default="data/formats",
        help="Folder containing the outputs from Session 1"
    )

    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of runs, including the first warm-up run"
    )

    args = parser.parse_args()

    if args.runs < 2:
        parser.error("--runs must be at least 2")

    formats_dir = Path(args.formats_dir)

    print(f"Formats folder: {formats_dir}")
    print(f"Number of runs: {args.runs}")

    spark = get_spark("FileFormatsReadSpeed")

    try:
        orders_schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("order_date", TimestampNTZType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
        ])

        orders_csv = (
            spark.read
            .option("header", True)
            .schema(orders_schema)
            .csv(str(formats_dir / "csv"))
        )

        orders_csv.printSchema()

        benchmark_reads(orders_csv, "CSV", args.runs)
        orders_json = (
            spark.read
            .schema(orders_schema)
            .json(str(formats_dir / "json"))
        )

        benchmark_reads(orders_json, "JSON", args.runs)

        for folder, label in [
            ("parquet_snappy", "Parquet Snappy"),
            ("parquet_gzip", "Parquet Gzip"),
            ("parquet_zstd", "Parquet Zstd"),
        ]:
            orders_parquet = spark.read.parquet(
                str(formats_dir / folder)
            )

            orders_parquet.printSchema()
            benchmark_reads(orders_parquet, label, args.runs)

        print("\nCSV amount sum plan:")
        orders_csv.agg(
            F.sum("amount").alias("total_amount")
        ).explain()

        print("\nParquet amount sum plan:")
        orders_parquet.agg(
            F.sum("amount").alias("total_amount")
        ).explain()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()