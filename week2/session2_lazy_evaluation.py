"""Session 2 Lazy evaluation. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 2 Lazy evaluation')
    parser.add_argument("--input", default="data/orders_big.parquet")
    parser.add_argument("--keep-ui-open", action="store_true")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session2Lazyevaluation')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        start = perf_counter()
        done_amounts = (
            orders.filter(F.col("status") == "done")
            .filter(F.col("amount") > 100)
            .select("customer_id", "amount")
            .withColumn("double_amount", F.col("amount") * 2)
        )
        print(f"Plan building: {perf_counter() - start:.3f} seconds")

        start = perf_counter()
        done_amounts.show(5)
        print(f"Show: {perf_counter() - start:.3f} seconds")

        print("Physical plan: read from the bottom up")
        done_amounts.explain()

        # Grouped count builds a DataFrame; show below executes it.
        customer_counts = done_amounts.groupBy("customer_id").count()
        customer_counts.explain()
        customer_counts.explain(True)
        customer_counts.show(5)

        if args.keep_ui_open:
            print("Spark UI:", spark.sparkContext.uiWebUrl)
            print("From Windows, also try http://localhost:4040")
            input("Press Enter when you have finished exploring the Spark UI: ")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
