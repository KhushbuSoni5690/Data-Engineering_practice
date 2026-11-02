"""Run the review SQL or C1 practice SQL against an orders temp view."""
import argparse
import os
import sys
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/orders_big.parquet")
    parser.add_argument("--file", default="week2/review_queries.sql")
    parser.add_argument("--query", type=int)
    parser.add_argument("--rows", type=int, default=20)
    args = parser.parse_args()
    # These files use SQL syntax common to DuckDB and Spark.
    statements = duckdb.extract_statements(Path(args.file).read_text())
    if not statements:
        parser.error("The SQL file contains no queries")
    if args.query is not None and not 1 <= args.query <= len(statements):
        parser.error(f"--query must be between 1 and {len(statements)}")
    if args.rows < 1:
        parser.error("--rows must be positive")
    spark = SparkSession.builder.master("local[*]").appName("ReviewSQL").getOrCreate()
    try:
        spark.read.parquet(args.input).createOrReplaceTempView("orders_view")
        for number, statement in enumerate(statements, 1):
            if args.query is None or number == args.query:
                print(f"Query {number}")
                spark.sql(statement.query).show(args.rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
