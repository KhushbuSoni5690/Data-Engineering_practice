"""Session 5 Nulls types and schemas. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampNTZType, DecimalType, StringType,
)

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 5 Nulls types and schemas')
    parser.add_argument("--input", default="data/orders_big.parquet")
    parser.add_argument("--csv", default="data/orders_big.csv", help="Existing CSV file or Spark CSV folder")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session5Nullstypesandschemas')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        orders.printSchema()
        print("Original orders:", orders.count())
        missing_amount_orders = orders.filter(F.col("amount").isNull())
        print("Missing amounts:", missing_amount_orders.count())

        orders_dropped = orders.na.drop(subset=["amount"])
        print("Rows after dropping missing amounts:", orders_dropped.count())
        orders_filled_zero = orders.na.fill({"amount": 0})
        print("Rows after filling with zero:", orders_filled_zero.count())

        category_averages = orders.groupBy("category").agg(
            F.avg("amount").alias("category_avg")
        )
        category_averages.show()
        orders_with_avg = orders.join(category_averages, on="category", how="left")
        orders_filled_avg = orders_with_avg.withColumn(
            "amount", F.coalesce(F.col("amount"), F.col("category_avg"))
        )
        print("Rows after filling with category average:", orders_filled_avg.count())
        print("Remaining missing amounts:", orders_filled_avg.filter(
            F.col("amount").isNull()
        ).count())
        # Category averages are estimates, not actual revenue. Keep and flag nulls
        # in the real pipeline. An entirely null category has no average to use.

        orders_cast = orders.withColumn("amount_decimal", F.col("amount").cast("decimal(12,2)"))
        orders_cast.printSchema()
        test_amounts = spark.createDataFrame([("125.50",), ("abc",), (None,)], ["amount_text"])
        # try_cast returns null for a bad value instead of failing the job.
        test_amounts.withColumn(
            "amount_number", F.col("amount_text").try_cast("decimal(12,2)")
        ).show()

        orders_schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("order_date", TimestampNTZType(), True),
            StructField("amount", DecimalType(12, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
        ])
        orders_csv = (
            spark.read.option("header", True)
            .schema(orders_schema)
            .csv(Path(args.csv).as_posix())
        )
        orders_csv.printSchema()
        orders_csv.show(5)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
