"""Session 1 DataFrame basics. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 1 DataFrame basics')
    parser.add_argument("--input", default="data/orders_big.parquet")

    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session1DataFramebasics')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        print("Spark version:", spark.version)
        orders.printSchema()
        print("Orders:", orders.count())

        orders.select("order_id", "customer_id", "amount").show(5)
        filtered_orders = orders.filter(F.col("amount") > 500)
        filtered_orders.show(5)
        print("Orders greater than 500:", filtered_orders.count())

        for status in ["done", "pending"]:
            matching_orders = orders.filter(F.col("status") == status)
            print(status, "orders:", matching_orders.count())

        amount_with_tax = orders.withColumn("taxed_amount", F.col("amount") * 1.10)
        amount_with_tax.show(5)

        orders_with_month = orders.withColumn(
            "order_month", F.date_trunc("month", F.col("order_date"))
        )
        orders_with_month.show(5)

        renamed_orders = orders.withColumnRenamed("amount", "order_amount")
        renamed_orders.show(5)
        orders_without_category = orders.drop("category")
        orders_without_category.show(5)

        orders.groupBy("status").count().orderBy(F.col("count").desc()).show()
        orders.groupBy("category").count().orderBy(F.col("count").desc()).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
