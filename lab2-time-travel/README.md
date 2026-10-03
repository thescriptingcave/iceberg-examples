# Lab 2: Time Travel

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand versioning in Iceberg** and how snapshots work
2. **Query historical versions** of tables using time travel
3. **Compare data across different points in time**
4. **Implement point-in-time recovery** scenarios
5. **Understand use cases** for time travel in real-world scenarios

## 📚 Part 1: Understanding Versioning in Iceberg

### What is Time Travel?

Time travel is the ability to query data as it existed at a previous point in time. It's like having a time machine for your data!

```
┌──────────────────────────────────────────────────────────────────┐
│                    Time Travel Concept                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  NOW     │  │  -1Hr    │  │  -2Hr    │  │  -3Hr    │        │
│  │  (t=3)   │  │  (t=2)   │  │  (t=1)   │  │  (t=0)   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │                │
│       ▼             ▼             ▼             ▼                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Snapshot │  │ Snapshot │  │ Snapshot │  │ Snapshot │        │
│  │  snap-3  │  │  snap-2  │  │  snap-1  │  │  snap-0  │        │
│  │  (Alice) │  │  (Alice) │  │  (Alice) │  │  (Alice) │        │
│  │   Bob    │  │   Bob    │  │   Bob    │  │   Bob    │        │
│  │ Charlie  │  │ Charlie  │  │ Charlie  │  │  Dave    │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│       │             │             │             │                │
│       │             │             │             └─► Query t=0   │
│       │             │             └───────► Query t=1           │
│       │             └──────────────► Query t=2                   │
│       └────────────────────────────► Query t=3 (now)            │
│                                                                  │
│  All snapshots are accessible simultaneously!                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Understanding Snapshots

Each Iceberg table maintains a history of snapshots. Every commit (insert,
update, delete, schema change) writes a new metadata file; commits that change
data also add a new snapshot to it:

```
s3://warehouse/iceberg/tutorial/customers/
├── metadata/
│   ├── 00000-<uuid>.metadata.json   ← table created (no snapshot yet)
│   ├── 00001-<uuid>.metadata.json   ← snapshot 1 (first insert)
│   ├── 00002-<uuid>.metadata.json   ← snapshot 2 (update)
│   ├── 00003-<uuid>.metadata.json   ← snapshot 3 (current)
│   ├── snap-<snapshot-id>-<uuid>.avro  ← manifest list, one per snapshot
│   └── <uuid>-m0.avro                  ← manifests (lists of data files)
└── data/
    └── ... .parquet                    ← data files, shared between snapshots
```

Each metadata file contains the full list of snapshots so far, and Polaris
stores a pointer to the latest one. You can list the metadata files with
`SELECT * FROM lakehouse.tutorial.customers.metadata_log_entries`.

Each snapshot contains:
- **Snapshot ID**: Unique identifier (a large random number, e.g. `495579447181732729`)
- **Parent Snapshot ID**: Points to previous snapshot (or null)
- **Timestamp**: When snapshot was created
- **Summary**: Operation type, files added/removed, records affected
- **Manifest List**: Pointers to manifest files

### Snapshot Chain

Snapshots form a chain (like a linked list):

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ snap-0   │────▶│ snap-1   │────▶│ snap-2   │────▶│ snap-3   │
│ (base)   │     │ (insert) │     │ (update) │     │ (delete) │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     ▲               ▲               ▲               ▲
     │               │               │               │
     └───────────────┴───────────────┴───────────────┘
                     │
              Current snapshot pointer
                     │
              ┌───────▼────────┐
              │   Table root   │
              └────────────────┘
```

This chain enables:
- **Linear history**: Each snapshot points to its parent
- **Efficient queries**: No need to scan all data
- **Time travel**: Navigate backward through the chain
- **Parallel snapshots**: Multiple branches possible (advanced)

---

## 🛠️ Part 2: Querying Historical Data with Spark

The examples below assume a `spark` session created with the canonical snippet in
[common/connection-reference.md](../common/connection-reference.md#spark-in-a-jupyter-notebook)
and the `lakehouse.tutorial.customers` table from the earlier labs. The table
needs a few commits (for example an insert, an update and a delete) so there is
some history to travel through.

### Method 1: Query by Snapshot ID

Snapshot IDs are large random numbers, not 1, 2, 3 -- look them up first:

```python
# Find the snapshot IDs (oldest first)
snapshots = spark.sql("""
SELECT snapshot_id, committed_at, operation
FROM lakehouse.tutorial.customers.snapshots
ORDER BY committed_at
""")
snapshots.show(truncate=False)

first_snapshot_id = snapshots.first()["snapshot_id"]

# Query a specific snapshot by ID
spark.sql(f"""
SELECT * FROM lakehouse.tutorial.customers
VERSION AS OF {first_snapshot_id}
""").show()

# The ID may also be given as a string; a string can also name a branch or tag
spark.sql(f"""
SELECT * FROM lakehouse.tutorial.customers VERSION AS OF '{first_snapshot_id}'
""").show()
```

### Method 2: Query by Timestamp

```python
# Query as of a specific timestamp (Spark session time zone; UTC in this stack)
spark.sql("""
SELECT * FROM lakehouse.tutorial.customers
TIMESTAMP AS OF '2026-10-03 05:21:13'
""").show()

# Or using Spark timestamp functions (seconds since the epoch)
spark.sql("""
SELECT * FROM lakehouse.tutorial.customers
TIMESTAMP AS OF timestamp_seconds(1791004873)
""").show()
```

Use a timestamp taken from the `committed_at` column of the snapshots table.
`TIMESTAMP AS OF` returns the snapshot that was current at that moment; if the
timestamp is earlier than the table's first snapshot, the query fails with
`Cannot find a snapshot older than ...`.

### Method 3: Query Historical Data with DataFrame API

```python
# Read as of a timestamp with the DataFrame API (milliseconds since the epoch)
df = spark.read \
    .format("iceberg") \
    .option("as-of-timestamp", "1791004873000") \
    .load("lakehouse.tutorial.customers")

df.show()

# Or by snapshot ID
df = spark.read \
    .format("iceberg") \
    .option("snapshot-id", first_snapshot_id) \
    .load("lakehouse.tutorial.customers")

df.show()
```

### Listing Available Snapshots

```python
# Get snapshot history
snapshots = spark.sql("SELECT * FROM lakehouse.tutorial.customers.snapshots")
snapshots.show(truncate=False)

# Get specific snapshot details
snapshot = spark.sql(f"""
SELECT * FROM lakehouse.tutorial.customers.snapshots
WHERE snapshot_id = {first_snapshot_id}
""")
snapshot.show(truncate=False)

# Which snapshot was current when (includes rollbacks)
spark.sql("SELECT * FROM lakehouse.tutorial.customers.history").show(truncate=False)

# Every metadata file the table has had
spark.sql("SELECT * FROM lakehouse.tutorial.customers.metadata_log_entries").show(truncate=False)
```

---

## 🔍 Part 3: Querying Historical Data with Trino

Open the Trino CLI with `docker exec -it iceberg-trino trino`. In Trino the same
table is `iceberg.tutorial.customers`.

### Method 1: Query by Timestamp

```sql
-- Query data as of a specific timestamp (the value must be a TIMESTAMP literal, not a string)
SELECT * FROM iceberg.tutorial.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-10-03 05:21:13 UTC';

-- Without a time zone, the session time zone is used (UTC in this stack)
SELECT * FROM iceberg.tutorial.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-10-03 05:21:13';
```

Trino does not accept `FOR SYSTEM TIME AS OF`, and a plain string such as
`FOR TIMESTAMP AS OF '2026-10-03 05:21:13'` is rejected -- use a `TIMESTAMP` literal.

### Method 2: Query by Snapshot ID

Trino supports snapshot IDs directly with `FOR VERSION AS OF`. Metadata tables
use a `$` suffix and must be quoted:

```sql
-- Get snapshot history
SELECT snapshot_id, committed_at, operation
FROM iceberg.tutorial."customers$snapshots"
ORDER BY committed_at;

-- Which snapshot was current when
SELECT * FROM iceberg.tutorial."customers$history";

-- Then query a snapshot by its ID
SELECT * FROM iceberg.tutorial.customers
FOR VERSION AS OF 495579447181732729;
```

### Method 3: Comparing Current vs Historical Data

```sql
-- Get current data
SELECT * FROM iceberg.tutorial.customers;

-- Get data as of a specific time
SELECT * FROM iceberg.tutorial.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-10-03 05:21:13 UTC';

-- Compare row counts
SELECT
    (SELECT COUNT(*) FROM iceberg.tutorial.customers) AS current_count,
    (SELECT COUNT(*) FROM iceberg.tutorial.customers
     FOR TIMESTAMP AS OF TIMESTAMP '2026-10-03 05:21:13 UTC') AS historical_count;
```

### Method 4: Analyzing Changes Over Time

```sql
-- Count changes between snapshots
WITH current_data AS (
    SELECT * FROM iceberg.tutorial.customers
),
historical_data AS (
    SELECT * FROM iceberg.tutorial.customers
    FOR TIMESTAMP AS OF TIMESTAMP '2026-10-03 05:21:13 UTC'
)
SELECT
    'current' AS source,
    COUNT(*) AS count,
    SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) AS non_null_ids
FROM current_data
UNION ALL
SELECT
    'historical' AS source,
    COUNT(*) AS count,
    SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) AS non_null_ids
FROM historical_data;
```

---

## 📊 Part 4: Comparing Data Across Time

### Comparing Row Counts Over Time

```python
# Spark: Compare row counts at different times
snapshots = spark.sql("""
SELECT * FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at
""")

for row in snapshots.collect():
    snapshot_id = row['snapshot_id']
    timestamp = row['committed_at']
    summary = row['summary']

    print(f"Snapshot {snapshot_id}: {timestamp}")
    print(f"  Operation: {row['operation']}")
    print(f"  Added records: {summary.get('added-records', 0)}")
    print(f"  Added files: {summary.get('added-data-files', 0)}")

    # Query snapshot
    df = spark.read \
        .format("iceberg") \
        .option("snapshot-id", snapshot_id) \
        .load("lakehouse.tutorial.customers")

    print(f"  Row count: {df.count()}")
    print()
```

### Comparing Schema Changes

```python
# Every metadata file, and the snapshot that was current in it. A schema
# change writes a new metadata file but keeps the same snapshot.
spark.sql("""
SELECT timestamp, file, latest_snapshot_id
FROM lakehouse.tutorial.customers.metadata_log_entries
""").show(truncate=False)

# SQL time travel reads a snapshot with the schema it was written with,
# so the columns can differ from the current table
for row in snapshots.collect():
    df = spark.sql(f"""
    SELECT * FROM lakehouse.tutorial.customers VERSION AS OF {row['snapshot_id']}
    """)
    print(f"Snapshot {row['snapshot_id']}: {df.columns}")
```

> The DataFrame option `snapshot-id` reads the old data but with the
> table's *current* schema; use SQL `VERSION AS OF` to see the old schema.

### Finding When a Row Changed

```python
# Find when a specific row was last modified
from pyspark.sql.functions import col

# Get snapshot history with timestamps
snapshots = spark.sql("""
SELECT * FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at
""")

# For each snapshot, check if row exists
target_customer_id = 1

for row in snapshots.collect():
    snapshot_id = row['snapshot_id']

    try:
        df = spark.read \
            .format("iceberg") \
            .option("snapshot-id", snapshot_id) \
            .load("lakehouse.tutorial.customers")

        result = df.filter(col("customer_id") == target_customer_id).count()

        if result > 0:
            print(f"Snapshot {snapshot_id}: Row exists")
        else:
            print(f"Snapshot {snapshot_id}: Row doesn't exist")
    except Exception as e:
        print(f"Snapshot {snapshot_id}: Error - {e}")
```

---

## 🚨 Part 5: Point-in-Time Recovery

The scenarios below find "the snapshot before the mistake" as the parent of the
snapshot the mistake created: the newest row of the `history` table. (Don't
just take the second newest row of the `snapshots` table -- after a rollback it
also contains abandoned snapshots.)

### Scenario 1: Accidental Deletion

```python
from pyspark.sql.functions import col

# Simulate accidental deletion
spark.sql("DELETE FROM lakehouse.tutorial.customers WHERE customer_id = 1")

# Verify row is deleted
spark.sql("SELECT * FROM lakehouse.tutorial.customers WHERE customer_id = 1").show()

# Point-in-time recovery
# Find the snapshot before the deletion (the parent of the current snapshot)
last_good_snapshot = spark.sql("""
SELECT parent_id FROM lakehouse.tutorial.customers.history
ORDER BY made_current_at DESC
LIMIT 1
""").first()["parent_id"]

# Option 1: Roll the whole table back to that snapshot
# (metadata-only; later snapshots stay in the history)
spark.sql(f"""
CALL lakehouse.system.rollback_to_snapshot('tutorial.customers', {last_good_snapshot})
""")

# Option 2: Copy the historical snapshot into a separate table to inspect it
spark.read \
    .format("iceberg") \
    .option("snapshot-id", last_good_snapshot) \
    .load("lakehouse.tutorial.customers") \
    .writeTo("lakehouse.tutorial.customers_recovery") \
    .createOrReplace()

# Option 3: Re-insert only the lost row, keeping any later changes
# (use this instead of Option 1, not after it)
historical_data = spark.read \
    .format("iceberg") \
    .option("snapshot-id", last_good_snapshot) \
    .load("lakehouse.tutorial.customers")

customer_to_restore = historical_data.filter(col("customer_id") == 1)
customer_to_restore.writeTo("lakehouse.tutorial.customers").append()
```

`rollback_to_timestamp('tutorial.customers', TIMESTAMP '...')` does the same
using a point in time, and `set_current_snapshot` can move the table forward
again to a snapshot that a rollback left behind. In Trino the equivalent is
`ALTER TABLE iceberg.tutorial.customers EXECUTE rollback_to_snapshot(<snapshot_id>)`.

### Scenario 2: Accidental Update

```python
# Simulate accidental update
spark.sql("""
UPDATE lakehouse.tutorial.customers
SET email = 'corrupted@example.com' WHERE customer_id = 1
""")

# Find the snapshot before the corruption
last_good_snapshot = spark.sql("""
SELECT parent_id FROM lakehouse.tutorial.customers.history
ORDER BY made_current_at DESC
LIMIT 1
""").first()["parent_id"]

# Restore just that row from the historical snapshot with MERGE
spark.sql(f"""
MERGE INTO lakehouse.tutorial.customers t
USING (
    SELECT * FROM lakehouse.tutorial.customers VERSION AS OF {last_good_snapshot}
    WHERE customer_id = 1
) h
ON t.customer_id = h.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

### Scenario 3: Table Drop Recovery

`DROP TABLE` without `PURGE` removes the table from the Polaris catalog but
leaves its metadata and data files in the object store. If you know the last
metadata file you can register the table again, with its full snapshot history.
`DROP TABLE ... PURGE` deletes the files, and then there is nothing to recover.

```python
# Before the drop: note the current metadata file (or keep a record of it)
metadata_file = spark.sql("""
SELECT file FROM lakehouse.tutorial.customers.metadata_log_entries
ORDER BY timestamp DESC LIMIT 1
""").first()["file"]
print(metadata_file)
# s3://warehouse/iceberg/tutorial/customers/metadata/000NN-<uuid>.metadata.json

# Oops
spark.sql("DROP TABLE lakehouse.tutorial.customers")

# Recover: register the table again from its last metadata file
spark.sql(f"""
CALL lakehouse.system.register_table(
    table => 'tutorial.customers',
    metadata_file => '{metadata_file}'
)
""").show()

# Data and snapshots are back
spark.sql("SELECT * FROM lakehouse.tutorial.customers.snapshots").show()
```

If you did not note the path, list the table's `metadata/` folder in the bucket
and pick the `.metadata.json` file with the highest number.

---

## 🎯 Part 6: Use Cases for Time Travel

### Use Case 1: Compliance and Auditing

```python
# Get all changes to a customer record
customer_id = 1

# Get snapshots
snapshots = spark.sql("""
SELECT * FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at
""")

for row in snapshots.collect():
    snapshot_id = row['snapshot_id']
    timestamp = row['committed_at']

    # Get customer at this point in time
    df = spark.read \
        .format("iceberg") \
        .option("snapshot-id", snapshot_id) \
        .load("lakehouse.tutorial.customers")

    customer = df.filter(col("customer_id") == customer_id)

    if customer.count() > 0:
        print(f"{timestamp}: {customer.collect()[0]}")
```

### Use Case 2: Data Quality Issues

```python
# Detect when data quality issues started

# Get row counts over time
snapshots = spark.sql("""
SELECT * FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at
""")

for row in snapshots.collect():
    snapshot_id = row['snapshot_id']

    # Read snapshot
    df = spark.read \
        .format("iceberg") \
        .option("snapshot-id", snapshot_id) \
        .load("lakehouse.tutorial.customers")

    # Check for data quality issues
    null_count = df.filter(col("email").isNull()).count()
    total_count = df.count()
    if total_count == 0:
        continue

    null_percentage = null_count / total_count * 100

    if null_percentage > 5:
        print(f"⚠️ Snapshot {snapshot_id}: {null_percentage:.1f}% null emails")
    else:
        print(f"✓ Snapshot {snapshot_id}: {null_percentage:.1f}% null emails")
```

### Use Case 3: A/B Testing Analysis

```python
# Compare current vs test data
test_snapshot_id = first_snapshot_id  # the snapshot from the test period

# Get current data
current_data = spark.read \
    .format("iceberg") \
    .load("lakehouse.tutorial.customers")

# Get data from test period
test_data = spark.read \
    .format("iceberg") \
    .option("snapshot-id", test_snapshot_id) \
    .load("lakehouse.tutorial.customers")

# Compare metrics
print("Current data:")
current_data.describe().show()

print("Test data:")
test_data.describe().show()
```

---

## 📝 Summary

| Time Travel Method | Spark | Trino |
|-------------------|-------|-------|
| **By Snapshot ID** | `VERSION AS OF <id>` | `FOR VERSION AS OF <id>` |
| **By Timestamp** | `TIMESTAMP AS OF '...'` | `FOR TIMESTAMP AS OF TIMESTAMP '...'` |
| **DataFrame API** | `option("snapshot-id", ...)` | N/A |
| **List Snapshots** | `SELECT * FROM table.snapshots` | `SELECT * FROM "table$snapshots"` |
| **Roll back** | `CALL lakehouse.system.rollback_to_snapshot(...)` | `ALTER TABLE ... EXECUTE rollback_to_snapshot(...)` |

| Use Case | Recommended Method |
|----------|-------------------|
| **Point-in-time recovery** | Spark with snapshot ID |
| **Ad-hoc historical queries** | Trino with timestamp |
| **Compliance/auditing** | Spark with snapshot history |
| **Data quality analysis** | Spark with snapshot analysis |

---

## 🚀 Challenge Questions

1. **Can you time travel across different catalogs?**
2. **What happens if you try to time travel to a deleted snapshot?**
3. **Can you perform writes to historical snapshots?**
4. **How does time travel affect performance?**
5. **Can you compare two different snapshots in one query?**

---

## 📚 Additional Reading

- [Iceberg Spec: Snapshots](https://iceberg.apache.org/spec/#snapshots)
- [Spark Time Travel Documentation](https://iceberg.apache.org/docs/latest/spark-queries/#time-travel)
- [Trino Time Travel Documentation](https://trino.io/docs/current/connector/iceberg.html#time-travel-queries)
- [Connection Reference](../common/connection-reference.md)

---

**Time travel is one of Iceberg's most powerful features! Master it to unlock the full potential of your data lake.**