# Orders Data Engineering Practice

A hands-on data engineering project using Python, pandas, PySpark, DuckDB and SQL. It generates sample order data, cleans it, analyses spending patterns, and compares data storage formats.

## Setup

The Spark scripts run in Ubuntu through WSL. The development environment uses Python 3.12, Java 17 and PySpark 4.2.0.

From PowerShell, open Ubuntu:

```powershell
wsl -d Ubuntu
```

For a new Ubuntu environment, install Java and create a Python virtual environment:

```bash
sudo apt update
sudo apt install openjdk-17-jre-headless python3-venv
python3 -m venv ~/.venvs/data-engineering
```

Activate the environment, then install the project dependencies from the project root:

```bash
source ~/.venvs/data-engineering/bin/activate
python -m pip install -r requirements.txt
java -version
```

For later sessions, activate the existing environment before running scripts.

Run all commands below from the project root.

## Project structure

- `common/` — shared Spark setup and helpers for measuring folder sizes and counting files.
- `week1/` — pandas cleaning and DuckDB analysis.
- `week2/` — Spark DataFrame operations, execution plans, joins and window functions.
- `week3/` — null handling, schemas, UDF comparisons and the PySpark cleaning pipeline.
- `week4/` — file-format and compression comparisons.
- `practice/` — additional SQL exercises.
- `tests/` — automated tests.
- `notes/` — learning notes and experiment results.
- `data/` — local input and generated output files, excluded from Git.

## Missing-amount policy

Both cleaning pipelines preserve missing amounts as null and add an `is_amount_missing` flag. Filling unknown amounts with averages or zero could misrepresent revenue.

SQL `SUM` and `AVG` ignore null amounts. Totals therefore represent known amounts, while averages use only non-null amounts. A group containing only null amounts has a null total.

The null-handling exercises also demonstrate dropping rows and filling missing values, but these alternatives are not used in the saved pipeline output.

## Week 1: pandas and DuckDB

The pandas pipeline removes duplicate order IDs, standardises names and status values, parses dates, cleans amount values, and saves the results as Parquet.

Generate the sample data if it does not already exist. The generator overwrites its generated files.

```bash
mkdir -p data
python week1/make_data.py
```

Run the cleaning pipeline:

```bash
python week1/clean_orders.py --input data/orders_raw.csv --output data/orders_clean.parquet
```

Run the DuckDB queries:

```bash
python week1/run_queries.py
```

The queries cover:

- Total and average order amount by city.
- Top customers by spending within each city.
- Monthly totals and a running total.
- Days between each customer’s consecutive orders.
- Duplicate removal using a CTE and `ROW_NUMBER`.
- City revenue using the larger orders and customers datasets.

The final query requires the Week 2 datasets. Generate those before running all queries.

To display one query or change the number of displayed rows:

```bash
python week1/run_queries.py --query 3 --rows 50
```

Week 1 spending queries include all order statuses. Week 2 revenue queries include only orders with status `done`.

## Week 2: Spark basics, joins and windows

Generate the larger orders and customers datasets if they do not already exist:

```bash
python week2/make_big_data.py
```

Run the individual sessions:

```bash
python week2/session1_dataframe_basics.py
python week2/session2_lazy_evaluation.py
python week2/session3_aggregations_joins.py
python week2/session4_windows_sql.py
```

The sessions demonstrate:

- Selecting, filtering, renaming and adding columns.
- Aggregating and sorting results.
- Lazy evaluation and execution plans.
- Inner and left joins.
- Customer rankings, running totals and gaps between orders.
- Equivalent operations using the DataFrame API and Spark SQL.

To keep Spark running while inspecting its UI:

```bash
python week2/session2_lazy_evaluation.py --keep-ui-open
```

Open `http://localhost:4040` in a browser while the script is waiting. Press Enter in the terminal when finished.

The join exercise adds three temporary orders with unmatched customer IDs. An inner join excludes these orders; a left join retains them with null customer details.

City revenue results are compared with DuckDB using the same source files and filters.

Run the query for the top two customers by spending in each category:

```bash
python week2/run_spark_queries.py
```

This query includes only completed orders. It calculates customer totals first, then ranks customers within each category. Ties are resolved using the smaller customer ID.

## Week 3: Nulls, schemas and the PySpark pipeline

Run the null-handling and schema exercises:

```bash
python week3/session5_nulls_types_schemas.py --csv data/orders_big.csv
```

The CSV must contain columns in this order:

```text
order_id, customer_id, order_date, amount, status, category
```

If using a Spark-generated CSV folder, pass that folder instead:

```bash
python week3/session5_nulls_types_schemas.py --csv data/orders_big_csv
```

If only the Parquet input is available, create a CSV folder:

```bash
python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[*]').getOrCreate(); s.read.parquet('data/orders_big.parquet').write.mode('errorifexists').option('header', True).csv('data/orders_big_csv'); s.stop()"
```

Compare built-in Spark functions with a Python UDF:

```bash
python week3/session6_builtin_vs_udf.py
```

The comparison checks that both implementations produce the same category counts and measures their execution times across repeated runs.

Run the PySpark cleaning pipeline:

```bash
python week3/orders_pipeline.py --input data/orders_big.parquet --output data/orders_clean_spark
```

The pipeline:

1. Reads the input Parquet data.
2. Removes duplicate order IDs.
3. Trims and lowercases status values.
4. Preserves null amounts and flags them.
5. Adds an `order_month` column.
6. Writes the cleaned data as Parquet.

Missing amounts are counted during the write using Spark’s `Observation`, avoiding a separate counting action.

If duplicate order IDs contain different values, Spark does not guarantee which row is retained. The pandas pipeline keeps the first occurrence.

Spark writes an output folder containing Parquet part files and a `_SUCCESS` marker. The number of part files can vary.

Each pipeline run overwrites its specified output folder. Input and output paths must be different.

## Week 4: File formats and compression

Compare CSV, JSON and Parquet by measuring writing time, output folder size and data-file count:

```bash
python -m week4.session1_formats_compare --input data/orders_big.parquet --output-dir data/formats
```

The script writes the same dataset into separate folders using:

- CSV with headers.
- JSON.
- Parquet with Snappy compression.
- Parquet with Gzip compression.
- Parquet with Zstandard compression.

The output folders are overwritten when the script is rerun.

Results and observations are recorded in `notes/week4-5-answers.md`. Timings depend on execution order, caching and computer workload, so a single run does not establish which format is always fastest.

Compare reading performance across CSV, JSON and three Parquet compression formats:

```bash
python -m week4.session2_read_speed --formats-dir data/formats --runs 5
```

Measures row counts and amount sums, excludes the first run, and reports
median timings. Also displays CSV and Parquet execution plans to inspect
column pruning.

## SQL window examples

`practice/window_queries.sql` contains three examples:

1. Ranking categories by revenue within each month.
2. Finding each customer’s first dated order.
3. Calculating each completed order’s percentage of that customer’s known completed-order spending.

Run all examples:

```bash
python week2/run_spark_queries.py --file practice/window_queries.sql
```

Run a selected example:

```bash
python week2/run_spark_queries.py --file practice/window_queries.sql --query 2
```

Percentage calculations return null when the customer total is zero or unknown.

## Testing

Run the test suite:

```bash
python -m pytest tests/ -v
```

Tests use small hand-made DataFrames and temporary files rather than the full generated datasets.

They cover duplicate removal, status cleaning, null handling, month creation, query execution, write-time observations and storage helpers.

A shared pytest fixture manages the Spark session for tests that need it.

## What I learned

- Logging makes pipeline progress and data issues visible.
- Command-line arguments make scripts reusable with different paths.
- Spark transformations build execution plans, while actions trigger processing.
- Built-in Spark functions avoid the extra Python-worker overhead of Python UDFs.
- Explicit schemas make expected data types clear.
- Window functions support rankings, running totals and comparisons between rows.
- Parquet and its compression options can substantially reduce storage size.
- Small, focused tests help verify expected behaviour before processing larger datasets.
- Git branches and pull requests provide a way to review and track project changes.

Further reflections are in `notes/notes.md` and `notes/week2-3-answers.md`.