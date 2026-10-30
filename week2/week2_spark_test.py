from time import perf_counter

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week2SparkPractice")
    .getOrCreate()
)
print(spark.version)

orders = spark.read.parquet("data/orders_big.parquet")
orders.printSchema()
print(orders.count())

# Session 1: DataFrame basics

orders.select("order_id", "customer_id", "amount").show(5)

filtered_orders = orders.filter(F.col("amount") > 500)
filtered_orders.show(5)
print("Orders greater than 500:", filtered_orders.count())

done_orders = orders.filter(F.col("status") == "done")
print("Done orders:", done_orders.count())

pending_orders = orders.filter(F.col("status") == "pending")
print("Pending orders:", pending_orders.count())

amount_with_tax = orders.withColumn(
    "taxed_amount",
    F.col("amount") * 1.10,
)
amount_with_tax.show(5)

orders_with_month = orders.withColumn(
    "order_month",
    F.date_trunc("month", F.col("order_date")),
)
orders_with_month.show(5)

renamed_orders = orders.withColumnRenamed("amount", "order_amount")
renamed_orders.show(5)

orders.groupBy("status").count().orderBy(
    F.col("count").desc()
).show()

# Session 2: Transformations, actions and execution plans

start = perf_counter()

done_amounts = (
    orders
    .filter(F.col("status") == "done")
    .filter(F.col("amount") > 100)
    .select("customer_id", "amount")
    .withColumn("double_amount", F.col("amount") * 2)
)
print("Plan building:", perf_counter() - start, "seconds")

start = perf_counter()
done_amounts.show(5)
print("Show:", perf_counter() - start, "seconds")
done_amounts.explain()

customer_counts = done_amounts.groupBy("customer_id").count()
customer_counts.explain()
customer_counts.explain(True)
customer_counts.show(5)

print("Spark UI:", spark.sparkContext.uiWebUrl)
input("Press Enter when you have finished exploring the Spark UI: ")

# Session 3: Aggregations and joins

category_summary = (
    orders
    .filter(F.col("status") == "done")
    .groupBy("category")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("average_amount"),
    )
)
category_summary.show()

customers = spark.read.parquet("data/customers.parquet")
customers.printSchema()
customers.show(5)

orders_with_customers = orders.join(
    customers,
    on="customer_id",
    how="inner",
)
orders_with_customers.show(5)

city_summary = (
    orders_with_customers
    .filter(F.col("status") == "done")
    .groupBy("city")
    .agg(F.round(F.sum("amount"), 2).alias("city_revenue"))
)
city_summary.show()

orders_left = orders.join(
    customers,
    on="customer_id",
    how="left",
)
orders_left.show(5)

customers.agg(
    F.max("customer_id").alias("largest_customer_id")
).show()

extra_orders = (
    orders.orderBy("order_id").limit(3)
    .withColumn("customer_id", F.col("order_id") + 10000)
)
extra_orders.select("order_id", "customer_id").show()

test_orders = orders.unionByName(extra_orders)

test_inner = test_orders.join(
    customers, on="customer_id", how="inner"
)
test_left = test_orders.join(
    customers, on="customer_id", how="left"
)

print("Inner:", test_inner.count())
print("Left:", test_left.count())

test_left.filter(F.col("customer_id") > 10000).select(
    "order_id", "customer_id", "name", "city"
).show()

customer_spend = (
    orders_with_customers
    .filter(F.col("status") == "done")
    .groupBy("customer_id", "name")
    .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
    .orderBy(F.col("total_spend").desc())
)
customer_spend.show(10)

orders_with_customers.explain()
# Join plan observed: BroadcastHashJoin, Inner, BuildRight (customers).

city_summary.orderBy("city").show()

# Session 4: Window functions

# Top 3 orders by amount for each customer.
customer_window = Window.partitionBy("customer_id").orderBy(
    F.col("amount").desc(),
    F.col("order_id"),
)

ranked_orders = orders.withColumn(
    "order_rank",
    F.row_number().over(customer_window),
)
top_orders = ranked_orders.filter(F.col("order_rank") <= 3)
top_orders.orderBy("customer_id", "order_rank").show(9)

# Running total of completed-order spending for each customer.
spend_window = (
    Window.partitionBy("customer_id")
    .orderBy("order_date", "order_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

done_orders = orders.filter(F.col("status") == "done")

orders_with_running_total = done_orders.withColumn(
    "running_total",
    F.round(F.sum("amount").over(spend_window), 2),
)

orders_with_running_total.select(
    "customer_id", "order_id", "order_date", "amount", "running_total"
).orderBy("customer_id", "order_date", "order_id").show(10)

# Days between each customer's consecutive orders.
order_window = Window.partitionBy("customer_id").orderBy(
    "order_date", "order_id"
)

orders_with_previous = orders.withColumn(
    "previous_order_date",
    F.lag("order_date").over(order_window),
)

orders_with_gaps = orders_with_previous.withColumn(
    "days_between_orders",
    F.datediff(F.col("order_date"), F.col("previous_order_date")),
)

orders_with_gaps.select(
    "customer_id", "order_date", "previous_order_date",
    "days_between_orders"
).orderBy("customer_id", "order_date", "order_id").show(10)

# Spark SQL: register the DataFrame as a temporary view.

orders.createOrReplaceTempView("orders_view")

spark.sql("""
    SELECT customer_id, order_id, amount
    FROM orders_view
    LIMIT 5
""").show()

spark.sql("""
    SELECT customer_id,
           order_id,
           amount,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY amount DESC, order_id
           ) AS order_rank
    FROM orders_view
    ORDER BY customer_id, order_rank
    LIMIT 9
""").show()

spark.sql("""
    WITH ranked AS (
        SELECT customer_id,
               order_id,
               amount,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY amount DESC, order_id
               ) AS order_rank
        FROM orders_view
    )
    SELECT *
    FROM ranked
    WHERE order_rank <= 3
    ORDER BY customer_id, order_rank
""").show(9)

# Practice exercise to revisit:
# Find the top 2 customers by total spending within each category,
# counting only orders with status 'done'.
# Use CTEs and a window function.
# Break spending ties using the smaller customer_id first.
# Return category, customer_id, total_spend and customer_rank.
# Sort by category and customer_rank.

# Original unfinished attempt, kept for practice:
# spark.sql("""
#     WITH total_spending_customer AS (
#         SELECT category,
#                customer_id,
#                ROUND(SUM(amount), 2) AS total_spend
#                ROW_NUMBER() OVER (
#                    PARTITION BY customer_id
#                    ORDER BY amount DESC, order_id
#                ) AS customer_rank
#         FROM order_view
#     )
#     SELECT category, customer_id, total_spend, customer_rank
#     FROM total_spending_customer
#     WHERE customer_rank <= 2
#     ORDER BY category, customer_rank
# """).show(6)

# Find the top 2 highest-amount completed orders in each category.
spark.sql("""
    WITH highest_order AS (
        SELECT category,
               order_id,
               amount,
               ROW_NUMBER() OVER (
                   PARTITION BY category
                   ORDER BY amount DESC, order_id
               ) AS order_rank
        FROM orders_view
        WHERE status = 'done'
    )
    SELECT *
    FROM highest_order
    WHERE order_rank <= 2
    ORDER BY category, order_rank
""").show(12)

# Week 3, session 5: missing amounts

missing_amount_orders = orders.filter(F.col("amount").isNull())
print("Missing amounts count:", missing_amount_orders.count())

# Drop rows with missing amounts.
orders_dropped = orders.na.drop(subset=["amount"])
print("Orders after dropping missing amounts:", orders_dropped.count())

# Replace missing amounts with 0.
orders_filled_zero = orders.na.fill({"amount": 0})
print("Orders filled with 0:", orders_filled_zero.count())

# Calculate the category averages.
category_averages = orders.groupBy("category").agg(
    F.avg("amount").alias("category_avg")
)
category_averages.show()

orders_with_avg = orders.join(
    category_averages,
    on="category",
    how="left",
)
orders_with_avg.select("category", "amount", "category_avg").show(5)

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

spark.stop()