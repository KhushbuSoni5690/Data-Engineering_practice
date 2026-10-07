# Orders Data Engineering Practice

## What this project does

This project creates sample customer and order data for practising data engineering.

Python removes duplicate order IDs, cleans customer names and order statuses, converts dates, and removes dollar signs from amounts. It fills missing amounts with the average amount and missing cities with "unknown", then saves the cleaned data as a Parquet file.

DuckDB SQL queries analyse total and average spending by city, the top three customers in each city, monthly revenue and running totals, and the days between each customer's orders. The project also compares SQL and pandas duplicate removal to check that both keep the same rows.

## How to run

Run these commands from the project's main folder.

Install the required packages:

```powershell
python -m pip install pandas pyarrow duckdb pytest
```

Clean the order data:

```powershell
python week1/clean_orders.py --input data/orders_raw.csv --output data/orders_clean.parquet
```

Run the tests:

```powershell
python -m pytest
```

Run the SQL queries:

```powershell
python week1/run_queries.py
```
run_queries.py will only show the result of last query, to see other quieries results just comment the rest.

## What I learned

I learned how to use logging to report progress and highlight data issues, and argparse to pass input and output file paths through the command line.

I practised SQL window functions for ranking customers, calculating running totals, and finding previous order dates. I am beginning to understand when to use each function.

I also learned that Parquet stores data by column and supports compression, making it useful for large analytical datasets, especially when queries need only a few columns.
