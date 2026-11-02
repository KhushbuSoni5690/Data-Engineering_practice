# Orders Data Engineering Practice

A learning project that generates sample orders, cleans them with pandas and PySpark, and analyses them using DuckDB and Spark SQL.

## Setup

Use Ubuntu WSL for Spark. From PowerShell, enter `wsl -d Ubuntu`, then run the following from the project root in Ubuntu.

For an existing environment:

```bash
source ~/.venvs/data-engineering/bin/activate
java -version
python -m pip install pandas numpy pyarrow duckdb pytest pyspark
```

For a new Ubuntu environment, first install Java 17 and create a virtual environment:

```bash
sudo apt update
sudo apt install openjdk-17-jre-headless python3-venv
python3 -m venv ~/.venvs/data-engineering
source ~/.venvs/data-engineering/bin/activate
python -m pip install pandas numpy pyarrow duckdb pytest pyspark
java -version
```

The user environment used Python 3.12, Java 17 and PySpark 4.2.0. Keep an already working environment; you do not need to upgrade it. Local validation details for this update are in `VALIDATION.md`.

## Missing-amount policy

Both cleaning pipelines keep missing amounts as null and add `is_amount_missing`, because filling with averages or zero would misrepresent unknown revenue.

SUM and AVG ignore null amounts; reported totals represent known amounts, not a guarantee of complete revenue. An all-null group's total stays null. Re-run the Week 1 cleaner to replace previously average-filled output; historical totals will change. Week 3 Session 5 still demonstrates alternative fills for learning but does not save them into the pipeline output.

## Week 1: pandas and DuckDB

Generate sample data only if needed: the generator overwrites its generated data.

```bash
mkdir -p data
python week1/make_data.py
python week1/clean_orders.py --input data/orders_raw.csv --output data/orders_clean.parquet
python week1/run_queries.py
python week1/run_queries.py --query 3 --rows 50
```

The runner displays a result for every query by default. `--query` selects one statement using its position in the SQL file. `--rows` changes the display limit; no commenting-out is needed. The final query compares city revenue on the Week 2 files, so generate those first when running all seven queries. Week 1 spending exercises retain their original all-status scope; Week 2 revenue queries use done orders only.

## Week 2: Spark basics, plans, joins and windows

Generate the larger dataset only if it does not already exist:

```bash
python week2/make_big_data.py
```

Run the separate sessions in order:

```bash
python week2/session1_dataframe_basics.py
python week2/session2_lazy_evaluation.py
python week2/session2_lazy_evaluation.py --keep-ui-open
python week2/session3_aggregations_joins.py
python week2/session4_windows_sql.py
```

With `--keep-ui-open`, open http://localhost:4040 from Windows, then press Enter in the terminal when finished. Without the flag, Spark stops automatically.

The join experiment adds three temporary orders with unmatched customer IDs. Inner joins omit those orders; left joins retain them with null customer fields. Inspect `explain()` to see the join strategy selected for your run. Compare the city totals to query 7 in `week1/queries.sql` on the same files.

B4 uses two CTEs: customer totals first, then customer ranking within each category. Equal totals are ordered by smaller customer ID. Amounts are cast to decimal before summing for stable monetary ties.

```bash
python week2/run_spark_queries.py
```

## Week 3: nulls, UDFs and the pipeline

```bash
python week3/session5_nulls_types_schemas.py --csv data/orders_big.csv
python week3/session6_builtin_vs_udf.py
python week3/orders_pipeline.py --input data/orders_big.parquet --output data/orders_clean_spark
python -m pytest -v
```

Session 5 requires an existing CSV with the columns in this order: order_id, customer_id, order_date, amount, status, category. If your CSV is a Spark output folder, pass `--csv data/orders_big_csv` instead. If you only have Parquet, create a CSV folder once:

```bash
python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[*]').getOrCreate(); s.read.parquet('data/orders_big.parquet').write.mode('errorifexists').option('header', True).csv('data/orders_big_csv'); s.stop()"
```

The pipeline loads data, drops duplicate order IDs, standardises status, flags null amounts, adds `order_month`, and writes Parquet. Its warning counts missing amounts during the write with `Observation`, avoiding a separate counting action. If duplicate IDs have different values, Spark does not promise which row it retains; pandas keeps the first input occurrence.

The output folder contains part files and a `_SUCCESS` marker. The file count can vary. Each pipeline run overwrites the specified output folder. Do not use the input path as the output path.

## Window practice and review

C1 solutions are in `practice/window_queries.sql`. Try writing your own versions first, then compare and run:

```bash
python week2/run_spark_queries.py --file practice/window_queries.sql
python week2/run_spark_queries.py --file practice/window_queries.sql --query 2
```

1. Monthly category ranking partitions by month and orders by category revenue; equal revenues share a RANK.
2. First order partitions by customer and orders by date then order ID; orders with missing dates are excluded.
3. Percentage of known done-order spend partitions by customer with no ordering, so the denominator covers the whole customer group. Zero or unknown totals return null percentages.

Read `APPLY_UPDATE.md` before copying files and `practice/GIT_AND_REFLECTION.md` for C2 and C3. Provided solutions do not demonstrate independent practice, and a prepared script does not complete a Git pull request.

## What I learned

Logging reports progress and data issues; argparse supplies file paths. Spark transformations build plans and actions execute them. Built-ins can run inside Spark's execution engine, while Python UDFs introduce worker and data-transfer overhead. Parquet stores data by column and supports compression. Small test DataFrames let me check cleaning behaviour without scanning a large dataset.
