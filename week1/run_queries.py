"""Run every SQL statement, or select one by its one-based number."""
import argparse
from pathlib import Path

import duckdb


def run_queries(sql_path: str, query_number: int | None = None, rows: int = 20) -> None:
    sql = Path(sql_path).read_text(encoding="utf-8")
    with duckdb.connect() as connection:
        # DuckDB's parser handles semicolons inside strings and SQL comments.
        statements = connection.extract_statements(sql)
        if not statements:
            raise ValueError("The SQL file contains no statements")
        if query_number is not None and not 1 <= query_number <= len(statements):
            raise ValueError(f"Choose a query number from 1 to {len(statements)}")
        for number, statement in enumerate(statements, start=1):
            if query_number is not None and number != query_number:
                continue
            print(f"\nQuery {number} of {len(statements)}")
            result = connection.execute(statement.query).fetchdf()
            print(result.head(rows).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="week1/queries.sql")
    parser.add_argument("--query", type=int, help="One-based query number; omit to run all")
    parser.add_argument("--rows", type=int, default=20, help="Maximum displayed rows per result")
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    try:
        run_queries(args.file, args.query, args.rows)
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
