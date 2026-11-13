from datetime import date, datetime
import pytest
from pyspark.sql import SparkSession
from week3.orders_pipeline import clean_orders, transform_orders

def test_clean_orders_removes_duplicates(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, "done", 100.0),
            (1, "done", 100.0),
            (2, "pending", 50.0),
        ],
        ["order_id", "status", "amount"]
    )

    result = clean_orders(df)

    assert result.count() == 2
    assert result.select("order_id").distinct().count() == 2


def test_clean_orders_standardises_status(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, "  DONE  ", 100.0),
            (2, " Pending ", 50.0),
        ],
        ["order_id", "status", "amount"]
    )

    result = clean_orders(df)

    statuses = {
        row.order_id: row.status
        for row in result.select("order_id", "status").collect()
    }

    assert statuses == {1: "done", 2: "pending"}


def test_clean_orders_flags_missing_amounts(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, "done", None),
            (2, "done", 100.0),
            (3, "done", 0.0),
        ],
        "order_id long, status string, amount double"
    )

    result = clean_orders(df)

    flags = {
        row.order_id: row.is_amount_missing
        for row in result.select(
            "order_id", "is_amount_missing"
        ).collect()
    }

    assert flags == {1: True, 2: False, 3: False}

def test_clean_orders_preserves_null_amount(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "done", None)],
        "order_id long, status string, amount double"
    )

    result = clean_orders(df)
    rows = result.select("order_id", "amount").collect()

    assert len(rows) == 1
    assert rows[0].order_id == 1
    assert rows[0].amount is None


def test_transform_orders_sets_order_month(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, datetime(2025, 11, 25, 14, 30)),
            (2, datetime(2024, 2, 29, 10, 0)),
            (3, datetime(2026, 1, 1, 0, 0)),
        ],
        "order_id long, order_date timestamp_ntz"
    )

    result = transform_orders(df)

    months = {
        row.order_id: row.order_month
        for row in result.select("order_id", "order_month").collect()
    }

    assert months == {
        1: date(2025, 11, 1),
        2: date(2024, 2, 1),
        3: date(2026, 1, 1),
    }