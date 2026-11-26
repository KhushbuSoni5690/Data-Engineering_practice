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

## Session 5: In-memory partitions

The input had 5 partitions. repartition(4) produced 4, and
repartition(20) produced 20. coalesce(2) reduced those 20 to 2.

With repartition("category"), separate queries showed different
partition counts. The final counting-query plan confirmed that
AQE combined the category shuffle partitions into one.

With repartition(6, "category"), six partitions remained, but only
three contained rows:
- Partition 0: Beauty, Books and Home — 999,900 rows.
- Partition 3: Clothing — 333,785 rows.
- Partition 5: Electronics and Sport — 666,315 rows.

The categories had similar row counts, but different categories
hashed to the same partition, making the partition sizes uneven.
Partitioning by a column does not guarantee balanced workloads.

### Coalesce versus repartition

Initial pipeline-plus-write timings:
- coalesce(1): 20.636 seconds
- repartition(1): 14.110 seconds

In the subsequent Spark UI run, coalesce used 5 tasks followed
by 1 task. Repartition used 5 tasks, then 9 tasks, then 1 task.

Repartition added a shuffle but allowed the intermediate processing
to run across multiple tasks. Both versions finished with one writer.

Coalesce avoids an extra shuffle, but reducing to one partition can
also reduce parallelism in preceding work. Neither method is always
faster; execution order and caching can affect these measurements.

### AQE: parallelism first

AQE and shuffle-partition coalescing were enabled.
parallelismFirst was true, default parallelism was 8,
and the advisory partition size was 64 MiB.

With parallelismFirst enabled, Spark considers default parallelism
when choosing its coalescing target instead of simply targeting
64 MiB partitions.

This helps explain why the earlier stage retained several tasks.
The exact count also depends on shuffle-partition sizes, so eight
default parallel tasks does not require exactly eight partitions.


### Coalesce versus repartition

The full cleaning pipeline plus Parquet write took:
- coalesce(1): 20.636 seconds.
- repartition(1): 14.110 seconds.

In the Spark UI, the coalesce run had 5 tasks in an earlier
stage, followed by 1 task doing the remaining processing and writing.

The repartition run had stages with 5, 9 and 1 tasks.
Its extra shuffle allowed more processing to happen in parallel
before the final single writing task.

Avoiding a shuffle does not always mean faster execution.
These were single measurements, so they do not prove that
repartition is always faster.

### AQE and available parallelism

AQE and adaptive partition coalescing were enabled.
parallelismFirst was true, default parallelism was 8,
and the advisory partition size was 64 MiB.

My computer has 4 physical cores and 8 logical processors.
With local[*], Spark could run up to 8 tasks simultaneously.
Nine tasks can run across those slots as slots become available.

AQE uses runtime information to adjust shuffle partitions.
Partition counts can differ between queries because the amount
of data being processed can differ.

### What the timer measured

Spark evaluates transformations lazily. Each write triggered
the cleaning pipeline as well as saving its output.
The cleaned DataFrame was not cached, so both writes requested
execution of the pipeline.