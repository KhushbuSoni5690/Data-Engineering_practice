# Validation of this update

## Passed here

- Syntax parsing of every supplied Python file.
- Two pytest checks: pandas duplicate/null/zero/currency handling, and the DuckDB runner's all-query/selected-query behaviour (including semicolons in SQL strings/comments and an invalid query number).
- B4 and all three C1 SQL queries executed against a hand-made seven-row DuckDB fixture. Checks covered equal customer totals, done-status filtering, first orders, percentages, zero totals and null totals.

## Not verified here

The six Spark pytest cases could not pass in this execution environment: Python worker startup failed (daemon launch failure / worker exit). A separate Spark Parquet smoke attempt also did not complete successfully. Do not interpret this package as a fully passing Spark test run. Local attempts used Python 3.12, Java 17 and PySpark 4.0.1; your existing working Ubuntu environment has a different Spark version.

Your large data files and existing test files were not uploaded. Full-data session execution, cross-tool city-total comparison, the complete repository test suite and your Git operations still need to be checked on your computer.

```bash
source ~/.venvs/data-engineering/bin/activate
python -m pytest tests/test_review_updates.py -v
python -m pytest -v
```

The test file includes eight checks in total and leaves your existing tests untouched.

## Reference documentation consulted

- https://duckdb.org/docs/stable/clients/python/reference/ (statement parsing and execution)
- https://spark.apache.org/docs/4.0.1/ (runtime compatibility for local validation)
