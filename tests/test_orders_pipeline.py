from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from week3.orders_pipeline import clean_orders


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("OrdersPipelineTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    try:
        yield session
    finally:
        session.stop()


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