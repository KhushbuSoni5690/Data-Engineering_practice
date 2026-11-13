"""Regression checks for the review changes; independent of the large data files."""
import os
import sys
from datetime import datetime, date
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from week1.clean_orders import clean_orders as clean_pandas
from week1.run_queries import run_queries
from week3.orders_pipeline import clean_orders, transform_orders, save_data

os.environ["PYSPARK_PYTHON"] = sys.executable


def test_pandas_null_policy_and_duplicates() -> None:
    frame = pd.DataFrame({
        "order_id": [1, 1, 2, 3], "customer_name": [" alice "] * 4,
        "order_date": ["01/01/2025"] * 4, "amount": ["$12.50", "$12.50", None, "$0"],
        "status": [" DONE "] * 4, "city": [None] * 4,
    })
    result = clean_pandas(frame).set_index("order_id")
    assert len(result) == 3
    assert result.loc[1, "amount"] == 12.5
    assert pd.isna(result.loc[2, "amount"])
    assert result.loc[2, "is_amount_missing"]
    assert not result.loc[3, "is_amount_missing"]
    assert result.loc[1, "status"] == "done"


def test_spark_duplicates(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "done", 1.0), (1, "done", 1.0), (2, "done", 2.0)],
                                  ["order_id", "status", "amount"])
    assert clean_orders(frame).count() == 2


def test_spark_status(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, " DONE ", 1.0)], ["order_id", "status", "amount"])
    assert clean_orders(frame).first().status == "done"


def test_spark_nulls_and_zero(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "done", None), (2, "done", 0.0)],
                                  "order_id long, status string, amount double")
    rows = {row.order_id: row for row in clean_orders(frame).collect()}
    assert rows[1].amount is None and rows[1].is_amount_missing
    assert rows[2].amount == 0 and not rows[2].is_amount_missing


def test_month(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(datetime(2025, 2, 19),)], ["order_date"])
    assert transform_orders(frame).first().order_month == date(2025, 2, 1)


def test_write_observation(spark: SparkSession, tmp_path: Path, caplog) -> None:
    frame = spark.createDataFrame([(1, "done", None)],
                                  "order_id long, status string, amount double")
    save_data(clean_orders(frame), str(tmp_path / "output"))
    assert "Found 1 orders with missing amounts" in caplog.text
    row = spark.read.parquet(str(tmp_path / "output")).first()
    assert row.amount is None and row.is_amount_missing


def test_query_runner_all_and_selected(tmp_path: Path, capsys) -> None:
    path = tmp_path / "sample.sql"
    path.write_text("-- A semicolon ; in a comment\nSELECT 'a;b' AS first_value; SELECT 7 AS second_value;")
    run_queries(str(path))
    output = capsys.readouterr().out
    assert "Query 1 of 2" in output and "Query 2 of 2" in output
    run_queries(str(path), 2)
    output = capsys.readouterr().out
    assert "Query 1 of 2" not in output and "second_value" in output
    with pytest.raises(ValueError):
        run_queries(str(path), 3)


def test_review_sql(spark: SparkSession) -> None:
    data = [
        (1, 1, datetime(2025, 1, 1), 30.0, "done", "Books"),
        (2, 1, datetime(2025, 1, 2), 70.0, "done", "Books"),
        (3, 2, datetime(2025, 1, 1), 100.0, "done", "Books"),
        (4, 3, datetime(2025, 1, 1), 90.0, "done", "Books"),
        (5, 3, datetime(2025, 1, 1), 999.0, "cancelled", "Books"),
        (6, 4, datetime(2025, 1, 1), 0.0, "done", "Home"),
        (7, 5, datetime(2025, 2, 1), None, "done", "Home"),
    ]
    frame = spark.createDataFrame(data, "order_id long, customer_id long, order_date timestamp, amount double, status string, category string")
    frame.createOrReplaceTempView("orders_view")
    b4 = duckdb.extract_statements((ROOT / "week2/review_queries.sql").read_text())[0].query
    rows = spark.sql(b4).filter("category = 'Books'").collect()
    assert [(r.customer_id, r.customer_rank) for r in rows] == [(1, 1), (2, 2)]
    queries = duckdb.extract_statements((ROOT / "practice/window_queries.sql").read_text())
    ranks = spark.sql(queries[0].query).collect()
    assert next(r for r in ranks if r.category == "Books").category_rank == 1
    firsts = spark.sql(queries[1].query).collect()
    assert next(r for r in firsts if r.customer_id == 1).order_id == 1
    percentages = {r.order_id: r for r in spark.sql(queries[2].query).collect()}
    assert percentages[1].percentage_of_customer_spend == 30
    assert percentages[2].percentage_of_customer_spend == 70
    assert percentages[6].percentage_of_customer_spend is None
    assert percentages[7].percentage_of_customer_spend is None
    assert 5 not in percentages
