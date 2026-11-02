"""Session 3 Aggregations and joins. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 3 Aggregations and joins')
    parser.add_argument("--input", default="data/orders_big.parquet")
    parser.add_argument("--customers", default="data/customers.parquet")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session3Aggregationsandjoins')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        category_summary = (
            orders.filter(F.col("status") == "done")
            .groupBy("category")
            .agg(
                F.round(F.sum("amount"), 2).alias("total_amount"),
                F.round(F.avg("amount"), 2).alias("average_amount"),
            )
        )
        category_summary.orderBy("category").show()

        customers = spark.read.parquet(Path(args.customers).as_posix())
        customers.printSchema()
        customers.show(5)
        orders_with_customers = orders.join(customers, on="customer_id", how="inner")
        orders_with_customers.show(5)

        city_summary = (
            orders_with_customers.filter(F.col("status") == "done")
            .groupBy("city")
            .agg(F.round(F.sum("amount"), 2).alias("city_revenue"))
        )
        city_summary.orderBy("city").show()
        orders_left = orders.join(customers, on="customer_id", how="left")
        orders_left.show(5)

        # Generate IDs beyond the largest customer ID, so none can match.
        max_customer_id = customers.agg(F.max("customer_id")).first()[0]
        print("Largest customer ID:", max_customer_id)
        extra_orders = (
            orders.orderBy("order_id").limit(3)
            .withColumn("customer_id", F.col("order_id") + max_customer_id)
        )
        extra_orders.select("order_id", "customer_id").show()
        # These extra rows are only for the join experiment; nothing is saved.
        test_orders = orders.unionByName(extra_orders)
        test_inner = test_orders.join(customers, on="customer_id", how="inner")
        test_left = test_orders.join(customers, on="customer_id", how="left")
        print("Inner:", test_inner.count())
        print("Left:", test_left.count())
        test_left.filter(F.col("customer_id") > max_customer_id).select(
            "order_id", "customer_id", "name", "city"
        ).show()

        customer_spend = (
            orders_with_customers.filter(F.col("status") == "done")
            .groupBy("customer_id", "name")
            .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
            .orderBy(F.col("total_spend").desc(), "customer_id")
        )
        customer_spend.show(10)
        print("Look for BroadcastHashJoin or SortMergeJoin:")
        orders_with_customers.explain()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
