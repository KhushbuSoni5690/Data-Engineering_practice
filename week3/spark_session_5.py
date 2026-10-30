import os
import sys
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("NullsTypesAndUDFs")
    .getOrCreate()
)
print(spark.version)

input_path = Path("data") / "orders_big.parquet"
orders = spark.read.parquet(str(input_path))
orders.printSchema()
print(orders.count())

# Week 3, session 5: nulls, types and schemas

# Count orders with missing amounts.
missing_amount_orders = orders.filter(F.col("amount").isNull())
print("Missing amounts count:", missing_amount_orders.count())

# Drop rows with missing amounts.
orders_dropped = orders.na.drop(subset=["amount"])
print("Orders after dropping missing amounts:", orders_dropped.count())

# Replace missing amounts with 0.
orders_filled_zero = orders.na.fill({"amount": 0})
print("Orders filled with 0:", orders_filled_zero.count())

# Calculate the average amount for each category.
category_averages = orders.groupBy("category").agg(
    F.avg("amount").alias("category_avg")
)
category_averages.show()

orders_with_avg = orders.join(
    category_averages,
    on="category",
    how="left",
)

orders_with_avg.select(
    "category", "amount", "category_avg"
).show(5)

orders_filled_avg = orders_with_avg.withColumn(
    "amount",
    F.coalesce(F.col("amount"), F.col("category_avg")),
)

print(
    "Orders filled with category averages:",
    orders_filled_avg.count(),
)
print(
    "Orders still missing amounts:",
    orders_filled_avg.filter(F.col("amount").isNull()).count(),
)

# Category averages are estimates, not actual payments.
# For revenue reporting, keep known amounts and flag missing values.

orders_cast = orders.withColumn(
    "amount_decimal",
    F.col("amount").cast("decimal(12,2)"),
)
orders_cast.printSchema()

test_amounts = spark.createDataFrame(
    [("125.50",), ("abc",), (None,)],
    ["amount_text"],
)
test_amounts.show()

# try_cast returns null for an invalid value such as "abc".
test_cast = test_amounts.withColumn(
    "amount_number",
    F.col("amount_text").try_cast("decimal(12,2)"),
)
test_cast.show()

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", TimestampNTZType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True),
])

orders_csv = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("data/orders_big.csv")
)
orders_csv.printSchema()
orders_csv.show(5)

# Week 3, session 6: built-in functions and Python UDFs

orders_strings = orders.withColumn(
    "status_clean",
    F.lower(F.trim(F.col("status"))),
)
orders_strings.select("status", "status_clean").show(5)

orders_dates = orders.withColumn(
    "order_day",
    F.to_date(F.col("order_date")),
)
orders_dates.select("order_day", "order_date").show(5)

orders_dates = orders_dates.withColumn(
    "order_month_text",
    F.date_format(F.col("order_day"), "yyyy-MM"),
)
orders_dates.select("order_day", "order_month_text").show(5)

# Label orders using built-in functions.
orders_labelled = orders.withColumn(
    "order_size",
    F.when(F.col("amount").isNull(), "unknown")
    .when(F.col("amount") < 100, "small")
    .when(F.col("amount") < 500, "medium")
    .otherwise("large"),
)
orders_labelled.select("amount", "order_size").show(10)


# Label orders using a Python function.
def label_order(amount: float | None) -> str:
    if amount is None:
        return "unknown"
    elif amount < 100:
        return "small"
    elif amount < 500:
        return "medium"
    else:
        return "large"


print(label_order(50))
print(label_order(250))
print(label_order(600))
print(label_order(None))

label_order_udf = F.udf(
    label_order,
    StringType(),
    useArrow=False,
)

orders_udf = orders.withColumn(
    "order_size",
    label_order_udf(F.col("amount")),
)
orders_udf.select("amount", "order_size").show(10)

# Time both versions five times.
builtin_times = []
udf_times = []

for run in range(5):
    versions = [
        ("Built-in", orders_labelled, builtin_times),
        ("Python UDF", orders_udf, udf_times),
    ]

    # Alternate which version runs first.
    if run % 2 == 1:
        versions.reverse()

    results = {}

    for name, df, timings in versions:
        start = perf_counter()

        counts = df.groupBy("order_size").count().collect()

        elapsed = perf_counter() - start
        timings.append(elapsed)

        results[name] = {
            row.order_size: row["count"]
            for row in counts
        }

        print(f"Run {run + 1} — {name}: {elapsed:.3f} seconds")

    assert results["Built-in"] == results["Python UDF"]

# Skip the first timing for each version.
print(
    f"Built-in median: {median(builtin_times[1:]):.3f} seconds"
)
print(
    f"Python UDF median: {median(udf_times[1:]):.3f} seconds"
)

spark.stop()

# Built-in functions run directly in Spark's engine and can be optimised.
# Python UDFs add Python processing and data-transfer overhead.