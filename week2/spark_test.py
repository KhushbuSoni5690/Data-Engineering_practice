from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import date_trunc
from time import perf_counter
from pyspark.sql.functions import sum, avg, round, lag, datediff

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print(spark.version)

orders = spark.read.parquet("data\orders_big.parquet")
orders.printSchema()
print(orders.count())

# # orders.select("order_id", "customer_id", "amount").show(5)
# filtered_orders = orders.filter(col("amount") > 500)
# filtered_orders.show(5)
# print("orders grater than 500:", filtered_orders.count())


# done_orders = orders.filter(col("status") == "done")
# print("done orders", done_orders.count())

# pending_orders = orders.filter(col("status") == "pending")
# print("pending orders", pending_orders.count())

# amount_with_tex = orders.withColumn("texed_amount", 
#                                     col("amount")* 1.10)
# # print(amount_with_tex.show(5))

# amount_with_tex.show(5)

# ## adding order_month column
# orders_with_month = orders.withColumn("order_month",
#                                        date_trunc("month", col("order_date")))
# orders_with_month.show(5)

# # column renamed by .withcolumnrenamed 
# renamed_orders = orders.withColumnRenamed("amount", "order_amount")
# renamed_orders.show(5)

# orders.groupBy("status").count().orderBy(col("count").desc()).show()
# start = perf_counter()

# # understanding of transformation and action in spark
# done_amounts = orders.filter(col("status") == "done").filter(col("amount") > 100).select("customer_id", "amount").withColumn("double_amount", col("amount") * 2)
# print("Plan building:", perf_counter() - start, "seconds")

# start = perf_counter()
# done_amounts.show(5)
# print("Show:", perf_counter() - start, "seconds")
# done_amounts.explain()

# customer_counts = done_amounts.groupBy("customer_id").count()
# customer_counts.explain()

# customer_counts.explain(True)
# customer_counts.show(5)

# print("Spark UI:", spark.sparkContext.uiWebUrl)
# input("Press Enter when you have finished exploring the Spark UI: ")

# Session 3 of week 2 begins
# using aggregation 
category_summary = orders.filter(col("status") == "done").groupBy("category").agg(
    round(sum("amount"), 2).alias("total_amount"),
    round(avg("amount"), 2).alias("average_amount")
)

category_summary.show()

# customers data loading and start joins
customers = spark.read.parquet("data/customers.parquet")
customers.printSchema()
customers.show(5)

orders_with_customers = orders.join(customers, on="customer_id", how="inner")
orders_with_customers.show(5)

city_summary = (
    orders_with_customers
    .filter(col("status") == "done")
    .groupBy("city")
    .agg(round(sum("amount"), 2).alias("city_revenue"))
)
city_summary.show()

orders_left = orders.join(customers, on = "customer_id", how = "left")
orders_left.show(5)

from pyspark.sql.functions import max

customers.agg(max("customer_id").alias("largest_customer_id")).show()
extra_orders = (
    orders.orderBy("order_id").limit(3)
    .withColumn("customer_id", col("order_id") + 10000)
)

extra_orders.select("order_id", "customer_id").show()
test_orders = orders.unionByName(extra_orders)

test_inner = test_orders.join(customers, on = "customer_id", how = "inner")
test_left = test_orders.join(customers, on = "customer_id", how = "left")

print("Inner:", test_inner.count())
print("Left:", test_left.count())

test_left.filter(col("customer_id") > 10000).select(
    "order_id", "customer_id", "name", "city"
).show()

customer_spend = ( orders_with_customers
                  .filter(col("status") == "done")
                  .groupBy("customer_id", "name")
                  .agg(round(sum("amount"),2).alias("total_spend"))
                  .orderBy(col("total_spend").desc())
)

customer_spend.show(10)
orders_with_customers.explain()
# # Join plan: BroadcastHashJoin, Inner, BuildRight (customers).
city_summary.orderBy("city").show()


# session 3 week2 1. top 3 orders by amount
customer_window = Window.partitionBy("customer_id").orderBy(
    col("amount").desc(),
    col("order_id")
)
row_number().over(customer_window)

ranked_orders = orders.withColumn("order_rank",row_number().over(customer_window))
top_orders = ranked_orders.filter(col("order_rank")<=3)
top_orders.orderBy("customer_id", "order_rank").show(9)

# session 3 week 2 2. runnibg total for each customer
spend_window = (
    Window.partitionBy("customer_id")
    .orderBy("order_date", "order_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
done_orders = orders.filter(col("status") == "done")
orders_with_running_total = done_orders.withColumn(
    "running_total",
    round(sum("amount").over(spend_window), 2)
)

orders_with_running_total.select(
    "customer_id", "order_id", "order_date", "amount", "running_total"
).orderBy("customer_id", "order_date", "order_id").show(10)

# 3. find the days between each customer's consecutive orders using lag()
order_window = Window.partitionBy("customer_id").orderBy(
    "order_date","order_id"
)
orders_with_previous = orders.withColumn(
    "previous_order_date",lag("order_date").over(order_window)
)
orders_with_gaps = orders_with_previous.withColumn(
    "days_between_orders",
    datediff(col("order_date"), col("previous_order_date"))
)
orders_with_gaps.select(
    "customer_id", "order_date", "previous_order_date", "days_between_orders"
).orderBy("customer_id", "order_date", "order_id").show(10)





spark.stop()