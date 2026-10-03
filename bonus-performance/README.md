# Bonus Lab: Performance Optimization

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand partitioning strategies** for Iceberg tables
2. **Learn about file sizing** and organization
3. **Use Z-order indexing** for query optimization
4. **Implement compaction** for better performance
5. **Monitor table health** and identify issues

## 📚 Part 1: Understanding Performance Optimization

### The Performance Triangle

```
┌──────────────────────────────────────────────────────────────────┐
│         The Iceberg Performance Triangle                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Data Layout Optimization                    │  │
│  │                                                            │  │
│  │  ┌──────────────┐     ┌──────────────┐     ┌────────────┐ │  │
│  │ │  Partitioning│────▶│  File Sizing │────▶│  Z-Order   │ │  │
│  │ │              │     │              │     │            │ │  │
│  │ │ - Reduces    │     │ - Minimize   │     │ - Clusters │ │  │
│  │ │   scans      │     │   small files│     │   related  │ │  │
│  │ │              │     │ - Avoid      │     │   columns  │ │  │
│  │ └──────────────┘     └──────────────┘     └────────────┘ │  │
│  │        │                     │                  │          │
│  └────────┼─────────────────────┼──────────────────┼──────────┘
│           │                     │                  │
│           ▼                     ▼                  ▼
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Compaction & Maintenance                    │  │
│  │                                                            │  │
│  │  ┌──────────────┐     ┌──────────────┐     ┌────────────┐ │  │
│  │ │   Compaction │     │  Expiration  │     │ Statistics │ │  │
│  │ │              │     │              │     │            │ │  │
│  │ │ - Merge small│     │ - Remove     │     │ - Enable   │ │  │
│  │ │   files      │     │   old data   │     │   planning │ │  │
│  │ └──────────────┘     └──────────────┘     └────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Key Performance Metrics

| Metric | Good Value | Bad Value | Impact |
|--------|-----------|-----------|--------|
| **File size** | 128MB - 1GB | <1MB, >1GB | Query performance |
| **Partition size** | 1M - 100M rows | <1K, >1B | Partition pruning |
| **Small file count** | <10 per partition | >100 | Scan overhead |
| **Manifest files** | <10 per snapshot | >100 | Metadata overhead |
| **Snapshot age** | <7 days | >30 days | Metadata bloat |

---

## 🛠️ Part 2: Partitioning Strategies

### What is Partitioning?

Partitioning divides data into subdirectories based on column values:

```
s3://warehouse/iceberg/tutorial/partitioned_events/data/
├── timestamp_day=2024-01-01/
│   ├── 00000-...-00001.parquet
│   └── 00001-...-00001.parquet
├── timestamp_day=2024-01-02/
│   ├── 00000-...-00001.parquet
│   └── 00001-...-00001.parquet
└── timestamp_day=2024-01-03/
    ├── 00000-...-00001.parquet
    └── 00001-...-00001.parquet
```

When you query with `WHERE timestamp >= '2024-01-02' AND timestamp < '2024-01-03'`, Iceberg only reads the files for `2024-01-02`! It knows which files those are from the partition values recorded in its manifests -- it never lists directories, so the folder names are just for humans.

### Partitioning Strategies

| Strategy | Use Case | Example | Pros | Cons |
|----------|----------|---------|------|------|
| **Hourly** | High-volume, recent data | `date_hour=2024-01-01-10` | Fine granularity | Too many partitions |
| **Daily** | Daily analytics | `date=2024-01-01` | Good balance | Not for real-time |
| **Monthly** | Historical data | `month=2024-01` | Few partitions | Less granular |
| **Bucket** | High-cardinality columns | `bucket(user_id, 10)` | Even distribution | Hard to prune |
| **No partition** | Small tables | None | Simple | Full scans |

### Choosing the Right Partition

All Spark examples in this lab run in a JupyterLab notebook and assume you
have already created `spark` with the `SparkSession` snippet from the
[Connection Reference](../common/connection-reference.md). Tables live in the
`tutorial` namespace of the `lakehouse` catalog. The customer table here is
called `perf_customers` so it does not collide with the `customers` table you
built in Labs 1-3.

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.tutorial")

# Rule 1: Partition by time for time-series data
# Rule 2: For high-cardinality columns (IDs), use bucket() -- never one partition per value
# Rule 3: Avoid over-partitioning (too many small files)

# Example 1: Event data (hourly partitioning)
spark.sql("""
CREATE TABLE lakehouse.tutorial.events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP,
    metadata STRUCT<ip: STRING, browser: STRING>
) USING ICEBERG
PARTITIONED BY (hour(timestamp))
""")

# Example 2: Customer data (no partitioning, use bucket instead)
spark.sql("""
CREATE TABLE lakehouse.tutorial.perf_customers (
    customer_id INT,
    name STRING,
    email STRING,
    created_at TIMESTAMP
) USING ICEBERG
-- No partitioning for small tables or use bucket
-- PARTITIONED BY (bucket(customer_id, 10))
""")

# Example 3: Daily sales data (daily partitioning)
spark.sql("""
CREATE TABLE lakehouse.tutorial.sales (
    order_id INT,
    customer_id INT,
    product_id INT,
    amount DECIMAL(10,2),
    sale_date DATE
) USING ICEBERG
PARTITIONED BY (day(sale_date))
""")
```

### Partition Pruning Demonstration

```python
# Create a partitioned table
spark.sql("""
CREATE TABLE lakehouse.tutorial.partitioned_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP
) USING ICEBERG
PARTITIONED BY (day(timestamp))
""")

# Insert data with different dates
from datetime import datetime, timedelta

for i in range(10):
    date = datetime(2024, 1, 1) + timedelta(days=i)
    # 100 events per day, one INSERT (= one commit) per day
    rows = ",\n".join(
        f"('{i}_{j}', '{j}', 'click', TIMESTAMP '{date + timedelta(minutes=j):%Y-%m-%d %H:%M:%S}')"
        for j in range(100)
    )
    spark.sql(f"INSERT INTO lakehouse.tutorial.partitioned_events VALUES {rows}")

# Query with partition pruning.
# Filter on the timestamp column itself: Iceberg turns the range into
# "only the 2024-01-05 partition". (Careful: Spark's day() returns the day of
# the MONTH, so WHERE day(timestamp) = '2024-01-05' silently matches nothing.)
print("Query with WHERE clause (uses partition pruning):")
spark.sql("""
SELECT COUNT(*) FROM lakehouse.tutorial.partitioned_events
WHERE timestamp >= '2024-01-05' AND timestamp < '2024-01-06'
""").show()   # 100

# Query without WHERE clause (full scan)
print("\nQuery without WHERE clause (full scan):")
spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.partitioned_events").show()   # 1000

# One row per partition: how many records and files each one holds
print("\nPartition statistics:")
spark.sql("SELECT * FROM lakehouse.tutorial.partitioned_events.partitions").show()
```

---

## 🔍 Part 3: File Sizing and Organization

### The 128MB Rule

```
┌──────────────────────────────────────────────────────────────────┐
│              Optimal File Size                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │   1MB    │  │  10MB    │  │ 128MB    │  │  1GB     │        │
│  │   files  │  │  files   │  │  files   │  │  files   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │                │
│       ▼             ▼             ▼             ▼                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  Bad     │  │  Bad     │  │  Good    │  │  Bad     │        │
│  │ Too many │  │  Large   │  │  Balanced│  │  Oversized│       │
│  │   files  │  │  scans   │  │  mix of  │  │   scans   │        │
│  │ overhead │  │   slow   │  │  IO and  │  │   memory  │        │
│  │          │  │          │  │  network │  │          │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                                                                  │
│  Optimal: 128MB - 1GB per file (Iceberg's default target: 512MB) │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Loading Some Small Files

`perf_customers` is empty so far. Load it the way many real pipelines do --
lots of small appends -- so there is something to compact:

```python
for batch in range(20):
    spark.sql(f"""
    INSERT INTO lakehouse.tutorial.perf_customers
    SELECT id,
           concat('Customer ', id),
           concat('c', id, '@example.com'),
           timestamp'2024-01-01 00:00:00' + make_interval(0, 0, 0, CAST(id % 365 AS INT))
    FROM range({batch * 500}, {(batch + 1) * 500})
    """)
```

Each `INSERT` is written by several Spark tasks in parallel, and every task
writes its own file, so 20 inserts produce hundreds of files of a couple of KB
each (440 on a typical laptop).

### Checking File Sizes

```python
# Check file sizes in your table
spark.sql("SELECT file_path, file_size_in_bytes, record_count FROM lakehouse.tutorial.perf_customers.files").show(truncate=False)

# Get summary statistics
spark.sql("""
SELECT 
    count(*) as total_files,
    min(file_size_in_bytes) as min_size,
    avg(file_size_in_bytes) as avg_size,
    max(file_size_in_bytes) as max_size
FROM lakehouse.tutorial.perf_customers.files
""").show()
```

### Detecting Small Files

```python
# Find partitions with too many small files.
# (Only partitioned tables have a `partition` column in .files, so this uses
# partitioned_events from Part 2.)
spark.sql("""
SELECT 
    partition,
    count(*) as file_count,
    sum(file_size_in_bytes) as total_size,
    avg(file_size_in_bytes) as avg_size
FROM lakehouse.tutorial.partitioned_events.files
GROUP BY partition
HAVING count(*) > 10 OR avg(file_size_in_bytes) < 10000000  -- < 10MB
""").show()
```

### Compaction Strategies

Compaction is done with the `rewrite_data_files` procedure. Its `strategy`
argument picks how files are combined: `binpack` (the default) just merges
small files, `sort` also re-orders the rows while it rewrites them.

```python
# Compaction merges small files into larger ones

# Strategy 1: Bin-pack compaction (the default strategy)
spark.sql("""
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.tutorial.perf_customers'
)
""").show()
# rewritten_data_files_count=440, added_data_files_count=1

# Strategy 2: Sort compaction -- rewrite AND order rows by customer_id.
# 'rewrite-all' forces a rewrite even though Strategy 1 already left a single
# file. Without it, only files that qualify are rewritten (e.g. at least
# 'min-input-files' small files, or files outside the
# 'min-file-size-bytes' / 'max-file-size-bytes' range).
spark.sql("""
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.tutorial.perf_customers',
    strategy => 'sort',
    sort_order => 'customer_id ASC NULLS LAST',
    options => map('rewrite-all', 'true', 'target-file-size-bytes', '536870912')
)
""").show()
```

> There is no `compact_table` procedure in Iceberg -- compaction is always
> `rewrite_data_files`. The option names are exact: `rewrite-all` (not
> `rewrite-all-files`), `max-file-size-bytes` (not `max-file-size`), and the
> sort is given by `sort_order` (there is no `sort_by` argument).

### Snapshot and Metadata Maintenance

Compaction does not delete the old small files: they are still referenced by
older snapshots, so time travel keeps working. Cleaning up is a separate step.

```python
# Merge many small manifest files into fewer, larger ones
spark.sql("CALL lakehouse.system.rewrite_manifests('lakehouse.tutorial.perf_customers')").show()

# Expire snapshots older than a timestamp, but always keep the newest one.
# Arguments to CALL must be literals -- current_timestamp() is not accepted.
spark.sql("""
CALL lakehouse.system.expire_snapshots(
    table => 'lakehouse.tutorial.perf_customers',
    older_than => TIMESTAMP '2099-01-01 00:00:00',
    retain_last => 1
)
""").show()
# deleted_data_files_count shows the old small files finally being removed

spark.sql("SELECT count(*) FROM lakehouse.tutorial.perf_customers.snapshots").show()   # 1
```

> **`remove_orphan_files` does not work from Spark on this stack.** The
> procedure lists the table directory through Hadoop's file-system layer, and
> the Jupyter image has no Hadoop S3 connector, so it fails with
> `No FileSystem for scheme "s3"`. Use Trino's `remove_orphan_files` (below)
> instead.

### Doing the Same from Trino

Trino runs the same maintenance with `ALTER TABLE ... EXECUTE`
(`docker exec -it iceberg-trino trino`):

```sql
-- Compaction (bin-pack); optionally only part of the table
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE optimize;
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE iceberg.tutorial.partitioned_events EXECUTE optimize
    WHERE timestamp >= TIMESTAMP '2024-01-05 00:00:00';

-- Rewrite manifests
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE optimize_manifests;

-- Expire snapshots / delete unreferenced files. Both require a retention
-- threshold, and Trino refuses anything shorter than 7 days by default.
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Inspect the result
SELECT count(*) FROM iceberg.tutorial."perf_customers$files";
SELECT committed_at, operation FROM iceberg.tutorial."perf_customers$snapshots";
```

A shorter retention fails with `Retention specified (0.00s) is shorter than
the minimum retention configured in the system (7.00d)`. On a throwaway
tutorial table you can lower the limit for your session:

```sql
SET SESSION iceberg.expire_snapshots_min_retention = '0s';
ALTER TABLE iceberg.tutorial.perf_customers EXECUTE expire_snapshots(retention_threshold => '0s');
```

(Never do that with `remove_orphan_files` on a table something else is writing
to: files from an in-progress write look exactly like orphans.)

---

## 📊 Part 4: Z-Order Indexing

### What is Z-Order?

Z-order is a clustering technique that co-locates related data:

```
┌──────────────────────────────────────────────────────────────────┐
│              Z-Order Indexing                                   │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Without Z-Order:                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │  File 1  │  │  File 2  │  │  File 3  │                      │
│  │ (A1,B1)  │  │ (A2,B2)  │  │ (A3,B3)  │                      │
│  │ (A2,B2)  │  │ (A3,B3)  │  │ (A4,B4)  │                      │
│  │ (A3,B3)  │  │ (A4,B4)  │  │ (A5,B5)  │                      │
│  └──────────┘  └──────────┘  └──────────┘                      │
│       ▲              ▲              ▲                            │
│       │              │              │                            │
│  ┌────┴──────────────┴──────────────┴───────────────┐           │
│  │                Query: A=2, B=2                    │           │
│  │              Must scan ALL files!                 │           │
│  └───────────────────────────────────────────────────┘           │
│                                                                  │
│  With Z-Order:                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │  File 1  │  │  File 2  │  │  File 3  │                      │
│  │ (A1,B1)  │  │ (A2,B2)  │  │ (A3,B3)  │                      │
│  │ (A2,B2)  │  │ (A3,B3)  │  │ (A4,B4)  │                      │
│  │ (A1,B2)  │  │ (A2,B3)  │  │ (A3,B4)  │                      │
│  └──────────┘  └──────────┘  └──────────┘                      │
│       ▲              ▲              ▲                            │
│       │              │              │                            │
│  ┌────┴──────────────┴──────────────┴───────────────┐           │
│  │                Query: A=2, B=2                    │           │
│  │        Only scan File 2! (clustering helps)       │           │
│  └───────────────────────────────────────────────────┘           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Creating Z-Order Index

Z-ordering is not a separate procedure: it is the `sort` strategy of
`rewrite_data_files` with a `zorder(...)` sort order.

```python
# Z-order by columns that are often queried together
spark.sql("""
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.tutorial.perf_customers',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, created_at)',
    options => map('rewrite-all', 'true')
)
""").show()
```

### Z-Order Benefits

```python
# Compare query plans -- run this once BEFORE the z-order rewrite above and
# once AFTER it.
query = """
SELECT * FROM lakehouse.tutorial.perf_customers
WHERE customer_id = 100 AND created_at > '2024-01-01'
"""
spark.sql(query).explain()
```

Both plans show the filters pushed into the Iceberg scan
(`BatchScan ... [filters=... customer_id = 100, ...]`). The plan text looks
the same either way; what z-ordering changes is how many *files* that scan has
to open. Iceberg skips any file whose min/max range for `customer_id` and
`created_at` cannot contain a match, and clustering makes those ranges narrow.
With this lab's tiny table everything fits in one file, so you will only see
the difference on tables with many files.

---

## 🚨 Part 5: Monitoring Table Health

### Common Performance Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Too many small files** | Slow queries, high IO | Compaction |
| **Too many partitions** | Slow metadata operations | Coarsen partitioning |
| **Stale statistics** | Poor query plans | Trino: `ANALYZE iceberg.tutorial.t`; Spark: `CALL lakehouse.system.compute_table_stats(table => '...')` (Spark's own `ANALYZE TABLE` is not supported for Iceberg tables) |
| **Uncompacted deletes** | Slow deletes | Compaction |

### Monitoring Queries

```python
# Check for small files
spark.sql("""
SELECT 
    file_path,
    file_size_in_bytes,
    record_count
FROM lakehouse.tutorial.perf_customers.files
WHERE file_size_in_bytes < 10000000  -- < 10MB
ORDER BY file_size_in_bytes ASC
LIMIT 10
""").show()

# Check partition statistics (partitioned tables only)
spark.sql("""
SELECT 
    partition,
    count(*) as file_count,
    sum(file_size_in_bytes) as total_size
FROM lakehouse.tutorial.partitioned_events.files
GROUP BY partition
ORDER BY file_count DESC
LIMIT 10
""").show()

# Check snapshot history (summary is a map with keys like 'added-data-files')
spark.sql("""
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['added-data-files'] AS added_files,
    summary['added-records'] AS added_records
FROM lakehouse.tutorial.perf_customers.snapshots
ORDER BY committed_at DESC
LIMIT 10
""").show()
```

### Automated Maintenance

```python
# Schedule regular maintenance
import time
from datetime import datetime

def maintenance_check(table_name):
    """Check and report on table health"""
    
    # Check for small files
    small_files = spark.sql(f"""
    SELECT COUNT(*) as small_file_count
    FROM {table_name}.files
    WHERE file_size_in_bytes < 10000000
    """).collect()[0][0]
    
    # Check for partition count (.partitions has one row per partition;
    # an unpartitioned table has exactly one row)
    partition_count = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {table_name}.partitions
    """).collect()[0][0]
    
    print(f"Table: {table_name}")
    print(f"  Small files: {small_files}")
    print(f"  Partitions: {partition_count}")
    
    if small_files > 10:
        print("  ⚠️  Recommendation: Run compaction")
    if partition_count > 1000:
        print("  ⚠️  Recommendation: Consider coarsening partitioning")

# Run maintenance check
maintenance_check("lakehouse.tutorial.perf_customers")
maintenance_check("lakehouse.tutorial.partitioned_events")
```

---

## 🎯 Part 6: Hands-on Exercises

### Exercise 1: Partition Optimization

```python
# 1. Create a table with inappropriate partitioning
# CREATE TABLE lakehouse.tutorial.bad_partitioned (...)
# PARTITIONED BY (hour(event_time))

# 2. Rewrite with better partitioning
# CREATE TABLE lakehouse.tutorial.good_partitioned (...) PARTITIONED BY (day(event_time))

# 3. Compare query performance
# SELECT COUNT(*) FROM lakehouse.tutorial.bad_partitioned
#   WHERE event_time >= '2024-01-01' AND event_time < '2024-01-02'
# SELECT COUNT(*) FROM lakehouse.tutorial.good_partitioned
#   WHERE event_time >= '2024-01-01' AND event_time < '2024-01-02'
```

### Exercise 2: Compaction

```python
# 1. Check for small files
# SELECT * FROM lakehouse.tutorial.perf_customers.files WHERE file_size_in_bytes < 10000000

# 2. Run compaction
# CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.tutorial.perf_customers', ...)

# 3. Verify file sizes increased
# SELECT AVG(file_size_in_bytes) FROM lakehouse.tutorial.perf_customers.files
```

### Exercise 3: Z-Order Indexing

```python
# 1. Z-order the table
# CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.tutorial.perf_customers',
#     strategy => 'sort', sort_order => 'zorder(customer_id, created_at)')

# 2. Compare query performance
# SELECT * FROM lakehouse.tutorial.perf_customers WHERE customer_id = 100 AND created_at > '2024-01-01'
```

---

## 📝 Summary

| Optimization Technique | When to Use | Tool |
|----------------------|-------------|------|
| **Partitioning** | Time-series or high-cardinality data | CREATE TABLE |
| **File Sizing** | Many small files | Compaction |
| **Z-Order** | Multi-column queries | `rewrite_data_files` with `zorder(...)` |
| **Compaction** | Fragmented data | `rewrite_data_files` (Spark) / `optimize` (Trino) |
| **Snapshot Expiration** | Old data cleanup | `expire_snapshots` (Spark and Trino) |

### Performance Checklist

- [ ] File sizes between 128MB and 1GB
- [ ] Partition counts under 10,000
- [ ] No more than 100 small files per partition
- [ ] Z-order indexes on frequently queried columns
- [ ] Regular compaction schedule
- [ ] Snapshot expiration configured

---

## 🚀 Challenge Questions

1. **How do you choose between hourly vs daily partitioning?**
2. **What's the impact of over-partitioning on metadata?**
3. **Can you use Z-order with partitioned tables?**
4. **How often should you run compaction?**
5. **What statistics should you collect for optimal query plans?**

---

## 📚 Additional Reading

- [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Spark Procedures (`rewrite_data_files`, `expire_snapshots`, ...)](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Table Properties, incl. `write.target-file-size-bytes`](https://iceberg.apache.org/docs/latest/configuration/)
- [Trino Iceberg Connector: `ALTER TABLE EXECUTE`](https://trino.io/docs/current/connector/iceberg.html#alter-table-execute)

---

**Optimization is the key to production-grade Iceberg tables! Master these techniques to ensure your data pipeline runs fast and scales efficiently.**