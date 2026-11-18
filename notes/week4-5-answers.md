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