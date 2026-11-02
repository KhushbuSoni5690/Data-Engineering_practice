"""Session 4 Windows and Spark SQL. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 4 Windows and Spark SQL')
    parser.add_argument("--input", default="data/orders_big.parquet")

    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session4WindowsandSparkSQL')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        # Top three orders per customer, including all statuses.
        customer_window = Window.partitionBy("customer_id").orderBy(
            F.col("amount").desc(), "order_id"
        )
        ranked_orders = orders.withColumn("order_rank", F.row_number().over(customer_window))
        top_orders = ranked_orders.filter(F.col("order_rank") <= 3)
        top_orders.orderBy("customer_id", "order_rank").show(9)

        # Running spend includes completed orders only.
        spend_window = (
            Window.partitionBy("customer_id")
            .orderBy("order_date", "order_id")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        done_orders = orders.filter(F.col("status") == "done")
        orders_with_running_total = done_orders.withColumn(
            "running_total", F.round(F.sum("amount").over(spend_window), 2)
        )
        orders_with_running_total.select(
            "customer_id", "order_id", "order_date", "amount", "running_total"
        ).orderBy("customer_id", "order_date", "order_id").show(10)

        # Gaps between consecutive orders, including all statuses.
        order_window = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
        orders_with_previous = orders.withColumn(
            "previous_order_date", F.lag("order_date").over(order_window)
        )
        orders_with_gaps = orders_with_previous.withColumn(
            "days_between_orders", F.datediff("order_date", "previous_order_date")
        )
        orders_with_gaps.orderBy("customer_id", "order_date", "order_id").select(
            "customer_id", "order_date", "previous_order_date", "days_between_orders"
        ).show(10)

        orders.createOrReplaceTempView("orders_view")
        spark.sql("""
            SELECT customer_id, order_id, amount
            FROM orders_view
            LIMIT 5
        """).show()

        # The same top-three calculation, using a CTE in Spark SQL.
        spark.sql("""
            WITH ranked AS (
                SELECT customer_id, order_id, amount,
                       ROW_NUMBER() OVER (
                           PARTITION BY customer_id
                           ORDER BY amount DESC, order_id
                       ) AS order_rank
                FROM orders_view
            )
            SELECT * FROM ranked
            WHERE order_rank <= 3
            ORDER BY customer_id, order_rank
        """).show(9)

        # Second SQL rewrite: running spend on done orders.
        spark.sql("""
            SELECT customer_id, order_id, order_date, amount,
                   ROUND(SUM(amount) OVER (
                       PARTITION BY customer_id
                       ORDER BY order_date, order_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ), 2) AS running_total
            FROM orders_view
            WHERE status = 'done'
            ORDER BY customer_id, order_date, order_id
        """).show(10)

        # Completed practice exercise: top two done orders in each category.
        spark.sql("""
            WITH highest_order AS (
                SELECT category, order_id, amount,
                       ROW_NUMBER() OVER (
                           PARTITION BY category
                           ORDER BY amount DESC, order_id
                       ) AS order_rank
                FROM orders_view
                WHERE status = 'done'
            )
            SELECT * FROM highest_order
            WHERE order_rank <= 2
            ORDER BY category, order_rank
        """).show(12)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
