import argparse
from pathlib import Path
from time import perf_counter

from common.spark_session import get_spark
from common.storage_utils import dir_size_mb, count_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare file sizes and writing times"
    )

    parser.add_argument(
        "--input",
        default="data/orders_big.parquet",
        help="Input Parquet path"
    )

    parser.add_argument(
        "--output-dir",
        default="data/formats",
        help="Folder for the different output formats"
    )

    args = parser.parse_args()
    spark = get_spark("FileFormatsComparison")

    try:
        orders = spark.read.parquet(args.input)
        orders.printSchema()
        csv_path = Path(args.output_dir) / "csv"

        start = perf_counter()

        orders.write.mode("overwrite").option(
            "header", True
        ).csv(str(csv_path))

        elapsed = perf_counter() - start

        print(f"CSV writing time: {elapsed:.2f} seconds")
        print(f"CSV folder size: {dir_size_mb(csv_path):.2f} MB")
        print(f"CSV file count: {count_files(csv_path, '.csv')}")

        json_path = Path(args.output_dir) / "json"

        start = perf_counter()

        orders.write.mode("overwrite").json(str(json_path))

        elapsed = perf_counter() - start

        print(f"JSON writing time: {elapsed:.2f} seconds")
        print(f"JSON folder size: {dir_size_mb(json_path):.2f} MB")
        print(f"JSON file count: {count_files(json_path, '.json')}")

        parquet_path = Path(args.output_dir) / "parquet_snappy"

        start = perf_counter()

        orders.write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(str(parquet_path))

        elapsed = perf_counter() - start

        print(f"Parquet Snappy writing time: {elapsed:.2f} seconds")
        print(f"Parquet Snappy folder size: {dir_size_mb(parquet_path):.2f} MB")
        print(f"Parquet Snappy file count: {count_files(parquet_path, '.parquet')}")

        gzip_path = Path(args.output_dir) / "parquet_gzip"

        start = perf_counter()

        orders.write.mode("overwrite").option(
            "compression", "gzip"
        ).parquet(str(gzip_path))

        elapsed = perf_counter() - start

        print(f"Parquet Gzip writing time: {elapsed:.2f} seconds")
        print(f"Parquet Gzip folder size: {dir_size_mb(gzip_path):.2f} MB")
        print(f"Parquet Gzip file count: {count_files(gzip_path, '.parquet')}")

        zstd_path = Path(args.output_dir) / "parquet_zstd"

        start = perf_counter()

        orders.write.mode("overwrite").option(
            "compression", "zstd"
        ).parquet(str(zstd_path))

        elapsed = perf_counter() - start

        print(f"Parquet Zstd writing time: {elapsed:.2f} seconds")
        print(f"Parquet Zstd folder size: {dir_size_mb(zstd_path):.2f} MB")
        print(f"Parquet Zstd file count: {count_files(zstd_path, '.parquet')}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()