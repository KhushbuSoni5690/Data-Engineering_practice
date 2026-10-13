from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import date_trunc
from time import perf_counter

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
start = perf_counter()

# Your done_amounts = ... line goes here
# understanding of transformation and action in spark
done_amounts = orders.filter(col("status") == "done").filter(col("amount") > 100).select("customer_id", "amount").withColumn("double_amount", col("amount") * 2)
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


spark.stop()