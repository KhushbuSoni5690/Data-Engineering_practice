import argparse
from pathlib import Path

from common.spark_session import get_spark


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explore partitioning Parquet files on disk"
    )
    parser.add_argument(
        "--input",
        default="data/orders_clean_spark"
    )
    parser.add_argument(
        "--output-dir",
        default="data/partitioned"
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    spark = get_spark("DiskPartitioning")

    try:
        orders = spark.read.parquet(args.input)

        print(f"Input folder: {args.input}")
        orders.printSchema()
        print(f"Output base folder: {output_dir}")
        baseline_path = output_dir / "unpartitioned"

        orders.write.mode("overwrite").parquet(
            str(baseline_path)
        )

        print(f"Baseline saved to: {baseline_path}")
        category_path = output_dir / "by_category"

        (
            orders.write
            .mode("overwrite")
            .partitionBy("category")
            .parquet(str(category_path))
        )

        print(f"Category layout saved to: {category_path}")

        month_path = output_dir / "by_month"

        (
            orders.write
            .mode("overwrite")
            .partitionBy("order_month")
            .parquet(str(month_path))
        )

        print(f"Month layout saved to: {month_path}")

        customer_count = (
            orders.select("customer_id")
            .distinct()
            .count()
        )

        print(f"Distinct customer IDs: {customer_count:,}")

        total_rows = orders.count()

        print(f"Total rows: {total_rows:,}")
        print(
            "Average rows per customer: "
            f"{total_rows / customer_count:,.1f}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()