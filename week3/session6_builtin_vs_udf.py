"""Session 6 Builtins versus UDF. Run from the project root."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from statistics import median
from time import perf_counter
from pyspark.sql.types import StringType


def label_order(amount: float | None) -> str:
    if amount is None:
        return "unknown"
    if amount < 100:
        return "small"
    if amount < 500:
        return "medium"
    return "large"

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description='Session 6 Builtins versus UDF')
    parser.add_argument("--input", default="data/orders_big.parquet")

    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('Session6BuiltinsversusUDF')
        .getOrCreate()
    )
    try:
        input_path = Path(args.input)
        orders = spark.read.parquet(input_path.as_posix())
        orders_strings = orders.withColumn("status_clean", F.lower(F.trim(F.col("status"))))
        orders_strings.select("status", "status_clean").show(5)
        orders_dates = orders.withColumn("order_day", F.to_date("order_date"))
        orders_dates = orders_dates.withColumn("order_month_text", F.date_format("order_day", "yyyy-MM"))
        orders_dates.select("order_date", "order_day", "order_month_text").show(5)

        orders_labelled = orders.withColumn(
            "order_size",
            F.when(F.col("amount").isNull(), "unknown")
            .when(F.col("amount") < 100, "small")
            .when(F.col("amount") < 500, "medium")
            .otherwise("large"),
        )
        orders_labelled.select("amount", "order_size").show(10)
        label_order_udf = F.udf(label_order, StringType(), useArrow=False)
        orders_udf = orders.withColumn("order_size", label_order_udf(F.col("amount")))
        orders_udf.select("amount", "order_size").show(10)

        builtin_times = []
        udf_times = []
        for run in range(5):
            versions = [
                ("Built-in", orders_labelled, builtin_times),
                ("Python UDF", orders_udf, udf_times),
            ]
            if run % 2 == 1:
                versions.reverse()
            results = {}
            for name, df, timings in versions:
                start = perf_counter()
                # Grouping by the new label forces Spark to evaluate it on all rows.
                counts = df.groupBy("order_size").count().collect()
                elapsed = perf_counter() - start
                timings.append(elapsed)
                results[name] = {row.order_size: row["count"] for row in counts}
                print(f"Run {run + 1} — {name}: {elapsed:.3f} seconds")
            assert results["Built-in"] == results["Python UDF"]

        # Exclude the first timed run of each version as warm-up.
        print(f"Built-in median: {median(builtin_times[1:]):.3f} seconds")
        print(f"Python UDF median: {median(udf_times[1:]):.3f} seconds")
        # Built-ins avoid the Python worker processing and data transfer of this UDF.
        # This is a local experiment; timings vary between runs and machines.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
