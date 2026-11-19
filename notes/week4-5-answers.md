# Weeks 4–5: Learning notes

## Session 1: Comparing file formats


Format	Folder size (MB)	Write time (seconds)	Files
CSV	112.70	8.71	3
JSON	263.47	4.21	3
Parquet Snappy	19.98	3.36	3
Parquet Gzip	14.49	3.17	3
Parquet Zstd	13.52	2.66	3

JSON used the most disk space at 263.47 MB, while CSV took the longest to write at 8.71 seconds. Parquet Zstd was smallest and fastest in this run, at 13.52 MB and 2.66 seconds.
Parquet’s size decreased from 19.98 MB with Snappy to 14.49 MB with Gzip and 13.52 MB with Zstd.
One run cannot prove Zstd is always fastest because execution order, caching and computer workload can affect timings.


## Session 2: Comparing reading speeds

Both CSV and Parquet showed only amount in ReadSchema.

CSV still needs to scan the text rows to find the amount values.
Parquet stores values by column, so Spark can read the amount
column without reading the other columns’ values.

This helps explain why Parquet summed amount faster in my experiment.

I used five runs and excluded the first as warm-up, then calculated
the median of the remaining four runs.

| Format | Count median (seconds) | Sum median (seconds) |
|---|---:|---:|
| CSV | 0.526 | 0.889 |
| JSON | 1.192 | 1.387 |
| Parquet Snappy | 0.112 | 0.153 |
| Parquet Gzip | 0.097 | 0.130 |
| Parquet Zstd | 0.103 | 0.141 |

These measurements use amount as double across all five formats.
Timings can vary with execution order, caching and computer workload.

## Session 3: Schemas and schema changes

Batch1 has the original six columns. Batch2 adds coupon_code.

Without schema merging, my combined read did not include coupon_code.
With mergeSchema=True, Spark included the new column and filled it
with null for batch1 rows because those files never had that column.

Order 10 showed null, while order 1010 showed SAVE10.

Changing amount from double to string in batch3 caused
CANNOT_MERGE_SCHEMAS. Schema merging can include an added column,
but it cannot automatically resolve this conflicting type.

A real pipeline should validate incoming types and convert valid
values to the agreed type, while flagging or rejecting invalid values.

### CSV: inferred versus explicit schema

Both methods returned 2,000,000 rows.

| Method | Read + count time (seconds) |
|---|---:|
| Inferred schema | 2.684 |
| Explicit schema | 0.591 |

Inference selected integer for the IDs and timestamp for order_date.
My explicit schema used long for the IDs and timestamp_ntz for
order_date, matching the original Parquet schema.

Providing a schema avoids the extra work of discovering column types
and keeps the expected types consistent.

The explicit read was faster in this run, but this was a single
comparison and execution order and caching could affect the timings.

## Session 4: Inside a Parquet file

I inspected a Snappy Parquet part file containing 1,048,576 rows
in one row group. All six columns used Snappy compression.

The amount statistics in the first row group were:
- Minimum: 0.06
- Maximum: 981.36

These statistics are stored in the file’s metadata, so we can inspect
them without reading every order.

For a filter such as amount > 1000, Spark can use the maximum to
skip this row group because none of its values can match.
For amount > 500, the group might contain matches and needs checking.

This is row-group skipping using statistics, enabled by pushing
the filter down to the Parquet reader.