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

# PySpark orders pipeline

This pipeline reads orders from a Parquet file, removes duplicate order IDs, and standardises status values by removing extra spaces and converting them to lowercase. It logs a warning for missing amounts and keeps them as null to avoid adding estimated revenue. It also adds an order_month column and saves the cleaned data as Parquet files.

The script uses separate functions for loading, cleaning, transforming and saving data, with type hints, logging and command-line arguments. Two pytest tests check duplicate removal and status cleaning.

 ## How to run
Tested with Ubuntu WSL, Java 17 and PySpark 4.2.0. Run these commands from the project folder with your Python environment activated.

Run the pipeline:
python week3/orders_pipeline.py --input data/orders_big.parquet --output data/orders_clean_spark

Run the tests:
python -m pytest tests/test_orders_pipeline.py -v

The output folder contains Parquet part files and a _SUCCESS marker. This run created nine data files; the number may vary. Running the pipeline again overwrites the output folder.