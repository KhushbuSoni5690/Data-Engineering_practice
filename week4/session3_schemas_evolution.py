import argparse
from pathlib import Path
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explore schemas and schema changes"
    )
    parser.add_argument(
        "--input",
        default="data/orders_big.parquet"
    )
    parser.add_argument(
        "--output-dir",
        default="data/formats/evolution"
    )
    parser.add_argument(
        "--csv",
        default="data/formats/csv",
        help="CSV folder created in Session 1"
    )

    args = parser.parse_args()
    output_dir = Path(args.output_dir)

    spark = get_spark("SchemasEvolution")

    try:
        orders = spark.read.parquet(args.input)
        orders.printSchema()
        print(f"Output folder: {output_dir}")

        # Batch 1: original six columns.
        batch1 = orders.orderBy("order_id").limit(1000)
        batch1_path = output_dir / "batch1"

        batch1.write.mode("overwrite").parquet(str(batch1_path))

        print(f"Batch 1 saved to: {batch1_path}")

        # Batch 2: add a coupon_code column.
        batch2 = (
            orders.orderBy("order_id")
            .offset(1000)
            .limit(1000)
            .withColumn(
                "coupon_code",
                F.when(
                    F.col("order_id") % 10 == 0,
                    F.lit("SAVE10")
                ).otherwise(F.lit(None).cast("string"))
            )
        )

        batch2_path = output_dir / "batch2"

        batch2.write.mode("overwrite").parquet(str(batch2_path))

        batch2.printSchema()
        batch2.select("order_id", "coupon_code").show(10)

        print(f"Batch 2 saved to: {batch2_path}")

        # Compare reading with and without schema merging.
        print("\nWithout schema merging:")
        orders_default = (
            spark.read
            .option("mergeSchema", False)
            .parquet(str(batch1_path), str(batch2_path))
        )
        orders_default.printSchema()

        print("\nWith schema merging:")
        orders_merged = (
            spark.read
            .option("mergeSchema", True)
            .parquet(str(batch1_path), str(batch2_path))
        )
        orders_merged.printSchema()

        orders_merged.filter(
            F.col("order_id").isin(10, 1010)
        ).select(
            "order_id", "coupon_code"
        ).orderBy("order_id").show()

        # Batch 3: deliberately change amount to string.
        batch3 = (
            orders.orderBy("order_id")
            .offset(2000)
            .limit(1000)
            .withColumn("amount", F.col("amount").cast("string"))
        )

        batch3_path = output_dir / "batch3"

        batch3.write.mode("overwrite").parquet(str(batch3_path))

        print("\nBatch 3 schema:")
        batch3.printSchema()

        print("\nTrying to merge all three batches:")

        # Catch the intentional schema-conflict error.
        try:
            orders_all = (
                spark.read
                .option("mergeSchema", True)
                .parquet(
                    str(batch1_path),
                    str(batch2_path),
                    str(batch3_path)
                )
            )

            orders_all.select("amount").show(5)

        except Exception as error:
            print("Could not read all three batches together:")
            print(str(error)[:1500])

        # CSV: let Spark infer the column types.
        print("\nCSV with inferred schema:")

        start = perf_counter()

        inferred_csv = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(args.csv)
        )

        inferred_rows = inferred_csv.count()
        inferred_elapsed = perf_counter() - start

        inferred_csv.printSchema()
        print(f"Rows: {inferred_rows:,}")
        print(
            f"Inferred schema read + count: "
            f"{inferred_elapsed:.3f}s"
        )

        print("\nCSV with explicit schema:")

        orders_schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("order_date", TimestampNTZType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
        ])

        start = perf_counter()

        explicit_csv = (
            spark.read
            .option("header", True)
            .schema(orders_schema)
            .csv(args.csv)
        )

        explicit_rows = explicit_csv.count()
        explicit_elapsed = perf_counter() - start

        explicit_csv.printSchema()
        print(f"Rows: {explicit_rows:,}")
        print(
            f"Explicit schema read + count: "
            f"{explicit_elapsed:.3f}s"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()