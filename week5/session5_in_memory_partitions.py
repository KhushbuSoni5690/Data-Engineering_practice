import argparse
from pyspark.sql import functions as F
from common.spark_session import get_spark
from pathlib import Path
from time import perf_counter


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explore Spark in-memory partitions"
    )
    parser.add_argument(
        "--input",
        default="data/orders_big.parquet"
    )
    parser.add_argument(
        "--output-dir",
        default="data/partitioned/session5",
        help="Folder for the single-partition comparisons"
    )
    args = parser.parse_args()
    spark = get_spark("InMemoryPartitions")

    try:
        print(
            "AQE enabled:",
            spark.conf.get("spark.sql.adaptive.enabled")
        )
        print(
            "AQE partition coalescing:",
            spark.conf.get(
                "spark.sql.adaptive.coalescePartitions.enabled"
            )
        )
        print(
            "Parallelism first:",
            spark.conf.get(
                "spark.sql.adaptive.coalescePartitions.parallelismFirst"
            )
        )
        print(
            "Default parallelism:",
            spark.sparkContext.defaultParallelism
        )
        print(
            "Advisory partition size:",
            spark.conf.get(
                "spark.sql.adaptive.advisoryPartitionSizeInBytes"
            )
        )
        orders = spark.read.parquet(args.input)

        print(
            "Partitions after reading:",
            orders.rdd.getNumPartitions()
        )

        orders_4 = orders.repartition(4)
        print(
            "After repartition(4):",
            orders_4.rdd.getNumPartitions()
        )

        orders_20 = orders.repartition(20)
        print(
            "After repartition(20):",
            orders_20.rdd.getNumPartitions()
        )

        orders_2 = orders_20.coalesce(2)

        print(
            "After coalesce(2) from 20 partitions:",
            orders_2.rdd.getNumPartitions()
        )

        orders_by_category = orders.repartition(6, "category")

        print(
            "After repartition by category:",
            orders_by_category.rdd.getNumPartitions()
        )
        category_partition_rows = orders_by_category.withColumn(
            "partition_id",
            F.spark_partition_id()
        )

        partition_counts = (
            category_partition_rows
            .groupBy("partition_id")
            .count()
            .orderBy("partition_id")
        )

        results = partition_counts.collect()

        for row in results:
            print(
                f"Partition {row['partition_id']}: "
                f"{row['count']:,} rows"
            )

        partition_counts.explain()

        category_partition_rows.groupBy(
            "partition_id", "category"
        ).count().orderBy(
            "partition_id", "category"
        ).show(truncate=False)

        output_dir = Path(args.output_dir)

        cleaned_orders = (
            orders
            .dropDuplicates(["order_id"])
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .withColumn("is_amount_missing", F.col("amount").isNull())
            .withColumn(
                "order_month",
                F.trunc(F.col("order_date"), "month")
            )
        )

        start = perf_counter()

        cleaned_orders.coalesce(1).write.mode("overwrite").parquet(
            str(output_dir / "coalesce_1")
        )

        coalesce_elapsed = perf_counter() - start
        print(f"Coalesce(1) pipeline + write: {coalesce_elapsed:.3f}s")

        start = perf_counter()

        cleaned_orders.repartition(1).write.mode("overwrite").parquet(
            str(output_dir / "repartition_1")
        )

        repartition_elapsed = perf_counter() - start
        print(
            f"Repartition(1) pipeline + write: "
            f"{repartition_elapsed:.3f}s"
        )
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        input("Press Enter when you have finished inspecting the Spark UI...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()