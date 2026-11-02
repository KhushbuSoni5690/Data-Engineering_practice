import argparse
import logging
import os
import sys
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession, Observation
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable

def load_data(spark: SparkSession, input_path: str) -> DataFrame:
    logging.info("Loading orders from %s", input_path)
    return spark.read.parquet(input_path)

def clean_orders(df: DataFrame) -> DataFrame:
    logging.info("Removing duplicate order IDs and cleaning status values")
    cleaned = df.dropDuplicates(["order_id"])

    cleaned = cleaned.withColumn(
        "status",
        F.lower(F.trim(F.col("status")))
    )

    cleaned = cleaned.withColumn(
        "is_amount_missing",
        F.col("amount").isNull()
    )

    return cleaned

def transform_orders(df: DataFrame) -> DataFrame:
    logging.info("Adding order_month")

    return df.withColumn(
        "order_month",
        F.trunc(F.col("order_date"), "month")
    )

def save_data(df: DataFrame, output_path: str) -> None:
    logging.info("Saving cleaned orders to %s", output_path)

    observation = Observation("amount_quality")

    observed_df = df.observe(
        observation,
        F.count(
            F.when(F.col("is_amount_missing"), 1)
        ).alias("missing_amount_count")
    )

    observed_df.write.mode("overwrite").parquet(output_path)

    missing_amount_count = observation.get["missing_amount_count"]

    if missing_amount_count > 0:
        logging.warning(
            "Found %s orders with missing amounts; keeping them as null",
            missing_amount_count
        )

    logging.info("Cleaned orders saved successfully")

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Clean orders using PySpark")

    parser.add_argument("--input", required=True, help="Input Parquet path")
    parser.add_argument("--output", required=True, help="Output Parquet folder")

    parser.add_argument("--keep-ui-open", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("OrdersPipeline")
        .getOrCreate()
    )

    try:
        start_time = perf_counter()
        orders = load_data(spark, args.input)
        cleaned_orders = clean_orders(orders)
        transformed_orders = transform_orders(cleaned_orders)
        save_data(transformed_orders, args.output)
        elapsed = perf_counter() - start_time
        logging.info("Pipeline with write-time observation: %.2f seconds", elapsed)

        if args.keep_ui_open:
            print("Spark UI:", spark.sparkContext.uiWebUrl)
            input("Press Enter to stop Spark...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
