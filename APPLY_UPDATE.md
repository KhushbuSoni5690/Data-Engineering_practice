# Apply this update

This ZIP is a project update, not a complete repository. It includes no data, virtual environment or Git history. Extract it outside your repo first, then copy the files into the matching paths.

## Before copying

Run `git status` and `git diff --cached --stat`. Preserve your existing staged review work in a checkpoint commit on a review branch. If you are still on main, create a branch before committing. Do not use git reset --hard or git restore to clear your work.

The package updates `README.MD`, `.gitignore`, `notes.md`, `review_tracker`, `week1/clean_orders.py`, `week1/run_queries.py`, and `week3/orders_pipeline.py`. The data generators and Week 1 SQL are carried forward from your uploads. It adds session files, review/practice SQL, a SQL runner and a regression test file. It does not overwrite your existing test files.

## After copying

In Ubuntu, from the project root:

```bash
source ~/.venvs/data-engineering/bin/activate
python -m pytest -v
```

Run the six session scripts using the commands in README.MD, then run B4 and C1. Re-run the Week 1 cleaner to apply the new null policy to its Parquet output. Existing Week 1 totals may change because average-filled amounts are no longer used.

## Retire the old combined files only after checking replacements

The new sessions replace `week2/week2_spark_test.py` and `week3/spark_session_5.py`. After saving the original work in Git and confirming the new scripts run, remove those tracked old files:

```bash
git rm week2/week2_spark_test.py week3/spark_session_5.py
```

Git history retains their previous contents. This ZIP does not automatically delete them or commit anything on your computer.

## Checklist

- B3: Split code supplied; local runs, removal of old files and commit still required.
- B4: Two-stage customer totals/ranking query and regression check supplied.
- B5: Both pipelines keep and flag null amounts; README explains the decision.
- B6: DuckDB runner displays each result or selects a query by number.
- B7: README setup and per-week commands are organised in code blocks.
- C1: Three runnable example solutions supplied; independent rewrite and explanation remain yours.
- C2: Follow practice/GIT_AND_REFLECTION.md and record three actual merged PRs.
- C3: Complete the reflection prompts in notes.md after the exercises.

## Additional fix

Your uploaded pipeline had `cleaned = df`, so duplicates were not removed. This update restores `dropDuplicates(["order_id"])` and checks it in the new tests. It preserves the write-time Observation warning and makes the Spark UI pause optional via `--keep-ui-open`.
