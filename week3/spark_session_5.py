
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import date_trunc
from time import perf_counter
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import sum, avg, round, lag, datediff
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, DecimalType, StringType
)

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print(spark.version)

orders = spark.read.parquet("data\orders_big.parquet")
orders.printSchema()
print(orders.count())

# # week 3 session 5 begins:

# missing orders count
missing_amount_orders =  orders.filter(col("amount").isNull())
print("missing orders count:", missing_amount_orders.count())

# droping all missing amount columns:
orders_dropped = orders.na.drop(subset = ["amount"])
print("orders dropped count:", orders_dropped.count())

# replacing missing amount with 0
order_filled_zero = orders.na.fill({"amount":0})
print("orders filled with 0:", order_filled_zero.count())

# replacing missing values with average
category_averages = orders.groupBy("category").agg(
    avg("amount").alias("category_avg"))
category_averages.show()

# join the category_avarages with orders.
orders_with_avg = orders.join(
    category_averages,
    on="category",
    how="left"
)

orders_with_avg.select("category", "amount", "category_avg").show(5)

orders_filled_avg = orders_with_avg.withColumn(
    "amount",
    coalesce(col("amount"),col("category_avg"))
)

print("orders filled with category's avg values:", orders_filled_avg.count())

print("orders with missing amount values:", orders_filled_avg.filter(
    col("amount").isNull()).count())

# Filling missing amounts with category averages adds estimated values to revenue. 
# #These may differ from what customers actually paid, making the report misleading. 
# We should report known revenue and clearly flag missing amounts.

# orders_cast = orders.withColumn(
#     "amount_decimal",
#     col("amount").cast("decimal(12,2)")
# )
# orders_cast.printSchema()

# test_amounts = spark.createDataFrame(
#     [("125.50",), ("abc",), (None,)],
#     ["amount_text"]
# )

# test_amounts.show()

# # intentionally getting error to test the cast type
# test_cast = test_amounts.withColumn(
#     "amount_number",
#     col("amount_text").try_cast("decimal(12,2)")
# )

# test_cast.show()

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", TimestampNTZType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True)
])
orders_csv = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("data\orders_big.csv")
)

orders_csv.printSchema()
orders_csv.show(5)


spark.stop()