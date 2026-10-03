# Lab 1: ACID Transactions (Spark Implementation)

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand ACID properties** in distributed data systems
2. **Create Iceberg tables** using Spark
3. **Perform transactions** (INSERT, UPDATE, DELETE)
4. **Handle concurrent writes** and understand conflict resolution
5. **Verify ACID guarantees** through hands-on exercises
6. **Compare Spark with other engines** for transaction handling

---

## 📚 Part 1: Understanding ACID Transactions

### What is ACID?

**ACID** is an acronym that stands for four key properties that guarantee reliable database transactions:

| Property | Definition | Real-World Analogy |
|----------|------------|-------------------|
| **Atomicity** | All operations in a transaction succeed or fail together | Like a chemical reaction - either the entire reaction happens or none of it does |
| **Consistency** | The database remains in a valid state before and after a transaction | Like a balance scale - it must always balance, never tipped to one side |
| **Isolation** | Concurrent transactions don't interfere with each other | Like parallel testing labs - each lab works independently without affecting others |
| **Durability** | Committed transactions persist even after system failures | Like writing in permanent ink - once committed, it stays forever |

### Why ACID Matters in Distributed Systems

In traditional databases (like PostgreSQL, MySQL), ACID is easy because all data lives on one server. But in distributed systems (like Iceberg with Spark, Garage object storage, Polaris), it's much more complex:

```
┌──────────────────────────────────────────────────────────────────┐
│              ACID Challenges in Distributed Systems               │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Problem 1: Network Partitions                                  │
│  ┌──────────────┐      ┌──────────────┐                         │
│  │   Node A     │      │   Node B     │                         │
│  │  (Spark)     │      │  (Spark)     │                         │
│  │              │  ??? │              │                         │
│  │ Write data   │  ??? │ Read data    │                         │
│  └──────────────┘      └──────────────┘                         │
│         ▲                      ▲                                 │
│         │                      │                                 │
│      ┌──┴───┐              ┌──┴───┐                             │
│      │  S3  │              │  S3  │                             │
│      └──────┘              └──────┘                             │
│                                                                  │
│  Without ACID: Node A writes, network drops, Node B reads      │
│  partial data!                                                  │
│                                                                  │
│  With ACID: Network partition detected, write fails atomically │
│  └─────────────────────────────────────────────────────────────┘
│                                                                  │
│  Problem 2: Concurrent Writes                                   │
│  ┌──────────────┐      ┌──────────────┐                         │
│  │   Node A     │      │   Node B     │                         │
│  │              │      │              │                         │
│  │ UPDATE id=1  │      │ UPDATE id=1  │                         │
│  │   SET x=10   │      │   SET x=20   │                         │
│  └──────────────┘      └──────────────┘                         │
│                                                                  │
│  Without ACID: Both writes succeed, one overwrites the other   │
│  Result: x=20 (lost update to x=10)                            │
│                                                                  │
│  With ACID: Conflict detected, first write succeeds, second    │
│  either fails or gets merged properly                           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Iceberg's Approach to ACID

Iceberg achieves ACID guarantees through a clever architecture:

```
┌──────────────────────────────────────────────────────────────────┐
│              Iceberg's ACID Architecture                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Read Current Metadata                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Spark reads: "Current snapshot is snap-10"                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                               ▲                                  │
│                               │                                  │
│  Step 2: Prepare New Data                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Spark writes new data files: 00000-...-00003.parquet      │ │
│  │  Spark writes new manifest + manifest list (.avro)         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                               ▲                                  │
│                               │                                  │
│  Step 3: Create New Metadata                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Spark creates new metadata: 00011-<uuid>.metadata.json    │ │
│  │  Metadata points to: snap-11 (new snapshot)                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                               ▲                                  │
│                               │                                  │
│  Step 4: Commit to Catalog                                      │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Spark sends to Polaris: "Update from snap-10 to snap-11"  │ │
│  │  Polaris checks: "Is current still snap-10?"               │ │
│  │  If YES: Commit succeeds, update metadata pointer          │ │
│  │  If NO: Commit fails (concurrent modification detected)    │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Key Point: The "current snapshot" pointer acts as a lock!     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Understanding Snapshots and Metadata

Let's understand the Iceberg file structure:

```
s3://warehouse/iceberg/tutorial/customers/
├── data/                                   # Your actual data files
│   ├── customer_id=1/00000-...-00001.parquet
│   ├── customer_id=2/00000-...-00002.parquet
│   └── customer_id=3/00000-...-00003.parquet
└── metadata/                               # Everything else lives here
    ├── 00000-<uuid>.metadata.json          # Metadata version 0 (table created)
    ├── 00001-<uuid>.metadata.json          # Metadata version 1 (after first commit)
    ├── snap-<snapshot-id>-1-<uuid>.avro    # Manifest list (one per snapshot)
    └── <uuid>-m0.avro                      # Manifest (lists data files)
```

The exact location is chosen by Polaris; the `.files`, `.manifests`,
`.snapshots` and `.metadata_log_entries` metadata tables (Part 3) show the real
paths for your table.

Each metadata file contains:
- **Schema**: Column definitions and types
- **Partitions**: How data is partitioned
- **Snapshots**: List of all snapshots with their properties
- **Current Snapshot ID**: Points to the latest snapshot
- **Properties**: Table properties and configuration

Each snapshot (an entry in the metadata file) contains:
- **Snapshot ID**: Unique identifier
- **Parent Snapshot**: Points to previous snapshot (or null)
- **Timestamp**: When snapshot was created
- **Manifest List**: Path to the `snap-*.avro` file that lists the snapshot's manifests

### How Polaris Enforces ACID

Polaris is crucial for ACID in distributed systems:

| ACID Property | How Polaris Helps |
|---------------|-------------------|
| **Atomicity** | Polaris commits metadata updates atomically |
| **Consistency** | Polaris validates schema changes before committing |
| **Isolation** | Polaris uses optimistic locking (current snapshot ID check) |
| **Durability** | Polaris writes the new metadata pointer to PostgreSQL before responding; the files themselves are already in object storage |

---

## 🛠️ Part 2: Creating Your Iceberg Table with Spark

### Step 1: Start Spark Session

This is the canonical session from
[`common/connection-reference.md`](../../common/connection-reference.md) -- if
the two ever disagree, that page wins.

```python
from pyspark.sql import SparkSession

# The object-store key is generated at first boot; read it from the shared volume.
creds = dict(line.strip().split("=", 1)
             for line in open("/creds/garage-credentials.env") if "=" in line)

spark = (
    SparkSession.builder.appName("iceberg-tutorial")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Catalog: Polaris, over the Iceberg REST protocol
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog")
    .config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")   # Polaris catalog NAME
    .config("spark.sql.catalog.lakehouse.credential", "root:root")
    .config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
    # Storage: Garage, over the S3 protocol
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://objectstore:3900")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", creds["AWS_ACCESS_KEY_ID"])
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", creds["AWS_SECRET_ACCESS_KEY"])
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    # Polaris does not grant the root principal metrics reporting; skip it.
    .config("spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled", "false")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)
```

**What's happening here?**

| Config | Purpose |
|--------|---------|
| `spark.sql.extensions` | Enables Iceberg SQL extensions (`MERGE`, `CALL` procedures, `ALTER TABLE ... WRITE ORDERED BY`, ...) |
| `spark.sql.catalog.lakehouse` | Registers a Spark catalog called `lakehouse`, implemented by Iceberg's `SparkCatalog` |
| `spark.sql.catalog.lakehouse.type` | `rest`: talk to the catalog over the Iceberg REST protocol |
| `spark.sql.catalog.lakehouse.uri` | Polaris REST endpoint; must end in `/api/catalog` or every call is a 404 |
| `spark.sql.catalog.lakehouse.warehouse` | For Polaris this is the **catalog name** (`lakehouse`), not an S3 path |
| `spark.sql.catalog.lakehouse.credential` | OAuth2 client id and secret (`root:root`); Spark exchanges them for a bearer token |
| `spark.sql.catalog.lakehouse.scope` | `PRINCIPAL_ROLE:ALL` activates the root principal's roles; without it every call is a 403 |
| `spark.sql.catalog.lakehouse.io-impl` | Use Iceberg's native S3 client for data and metadata files |
| `spark.sql.catalog.lakehouse.s3.endpoint` | Garage's S3 API inside the Docker network |
| `spark.sql.catalog.lakehouse.s3.path-style-access` | Garage addresses buckets as `host/bucket`, not `bucket.host` |
| `spark.sql.catalog.lakehouse.s3.access-key-id` / `secret-access-key` | Garage's key, generated at first boot and read from `/creds/garage-credentials.env` (Garage has no STS, so Polaris cannot vend credentials) |
| `spark.sql.catalog.lakehouse.client.region` | The AWS SDK insists on a region; Garage accepts `us-east-1` |
| `spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled` | Off, because Polaris does not grant the root principal metrics reporting |
| `spark.sql.defaultCatalog` | Makes `lakehouse` the default, so `tutorial.customers` also works |

The lab uses the `tutorial` namespace, created in Lab 0. If it is missing, run
`spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.tutorial")`.

> **Already ran `spark-init.sql` in Lab 0?** No problem: it seeds the same three
> rows, and Step 4's `INSERT OVERWRITE` replaces them rather than appending, so
> you still end up with three rows. Run both steps in any order, as often as
> you like.
>
> The rest of the lab uses plain `INSERT INTO` on purpose -- Step 4's job is to
> establish a known starting point, and the later steps are about what happens
> when writes add, update and delete rows. Only the seed step needs to be
> repeatable.

### Step 2: Create an Iceberg Table

```python
# Create an Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.tutorial.customers (
    customer_id INT,
    name STRING,
    email STRING,
    created_at TIMESTAMP
) USING ICEBERG
PARTITIONED BY (customer_id)
""")

print("✅ Table 'lakehouse.tutorial.customers' created!")
```

**Understanding the syntax:**
- `CREATE TABLE IF NOT EXISTS` - Only create if table doesn't exist
- `lakehouse.tutorial.customers` - catalog `lakehouse`, namespace `tutorial`, table `customers`
- `customer_id INT` - Column definition
- `USING ICEBERG` - Specifies the table format
- `PARTITIONED BY (customer_id)` - Partition by customer_id (more on this in Lab 3)

### Step 3: Verify Table Creation

```python
# Show tables in the tutorial namespace
spark.sql("SHOW TABLES IN lakehouse.tutorial").show()

# Get table schema
spark.sql("DESCRIBE lakehouse.tutorial.customers").show()
```

### Step 4: Insert Initial Data

```python
# Insert sample data
spark.sql("""
INSERT OVERWRITE lakehouse.tutorial.customers VALUES
    (1, 'Alice Smith', 'alice@example.com', TIMESTAMP '2024-01-01 10:00:00'),
    (2, 'Bob Johnson', 'bob@example.com', TIMESTAMP '2024-01-02 11:00:00'),
    (3, 'Charlie Brown', 'charlie@example.com', TIMESTAMP '2024-01-03 12:00:00')
""")

print("✅ Initial data inserted!")
```

`INSERT OVERWRITE` replaces the table's contents, so re-running Step 4 leaves
the same three rows instead of appending a second copy. Iceberg has no primary
keys, so a plain `INSERT INTO` would happily give you six rows with duplicate
`customer_id`s on the second run.

> **Why `TIMESTAMP '...'`?** Iceberg tables use strict (ANSI) store assignment,
> so a plain string like `'2024-01-01 10:00:00'` is rejected with
> `Cannot safely cast 'created_at' "STRING" to "TIMESTAMP"`. Use a typed literal
> or an explicit `CAST`.

### Step 5: Read the Data

```python
# Query the table
result = spark.sql("SELECT * FROM lakehouse.tutorial.customers")
result.show()

# Count rows
count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.tutorial.customers").collect()[0]['count']
print(f"Total rows: {count}")
```

---

## 🔍 Part 3: Understanding Transactions in Spark

### What Happens During a Transaction?

Let's trace through what happens when you run an INSERT:

```
┌──────────────────────────────────────────────────────────────────┐
│           Spark INSERT Transaction Flow                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Spark Client                                               │
│     └── SQL: INSERT INTO customers VALUES (4, 'Dave', ...)     │
│                                                                  │
│  2. Spark SQL Engine                                           │
│     ├── Parses SQL                                             │
│     ├── Validates schema                                       │
│     └── Plans execution                                        │
│                                                                  │
│  3. Spark Executor                                             │
│     ├── Reads current metadata (via Polaris)                   │
│     │   └── Gets current snapshot: snap-1                      │
│     ├── Processes new data                                     │
│     ├── Creates new data file: part-0003.parquet              │
│     ├── Creates new manifest: m0003.avro                      │
│     └── Creates new metadata: v2.metadata.json                │
│                                                                  │
│  4. Commit to Polaris                                          │
│     ├── Sends: "Update snap-1 → snap-2"                       │
│     ├── Polaris checks: "Is current still snap-1?"            │
│     ├── Polaris confirms → Commits metadata                   │
│     └── Polaris returns: "Commit successful!"                  │
│                                                                  │
│  5. Spark Client                                               │
│     └── Returns: "1 row inserted"                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Transaction Isolation Levels

Readers always get **snapshot isolation**: a query reads one committed snapshot
from start to finish and never sees a half-finished write. For writers
(`UPDATE`, `DELETE`, `MERGE`), Iceberg lets you choose how strictly concurrent
commits are checked:

| Isolation Level | What It Means | Use Case |
|-----------------|---------------|----------|
| **serializable** (default) | Fails the commit if another writer added data files that could contain rows matching your condition | Correctness-critical updates |
| **snapshot** | Only fails if files you read were removed or rewritten; tolerates concurrent appends | Higher write concurrency |

The level is a table property, set per operation:

```python
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers SET TBLPROPERTIES (
    'write.update.isolation-level' = 'snapshot'   -- also write.delete.* and write.merge.*
)
""")
```

### Checking Transaction Status

Every committed transaction is a snapshot. Iceberg exposes them as metadata tables:

```python
# One row per committed transaction
spark.sql("""
SELECT committed_at, snapshot_id, parent_id, operation
FROM lakehouse.tutorial.customers.snapshots
ORDER BY committed_at
""").show(truncate=False)

# Which metadata.json file each commit produced
spark.sql("SELECT timestamp, file FROM lakehouse.tutorial.customers.metadata_log_entries").show(truncate=False)

# Data files in the current snapshot
spark.sql("SELECT file_path, record_count FROM lakehouse.tutorial.customers.files").show(truncate=False)
```

Other metadata tables: `.history`, `.manifests`, `.partitions`, `.refs`.

---

## 🚨 Part 4: Handling Concurrent Writes

### Scenario: Two Spark Sessions Writing Simultaneously

Let's simulate what happens when two processes try to update the same row at the same time.

```python
# Session 1 - Update Alice's email
from threading import Thread
import time

def session1_update():
    print("Session 1: Starting update...")
    time.sleep(1)  # Simulate some processing time
    spark.sql("""
    UPDATE lakehouse.tutorial.customers 
    SET email = 'alice.new@example.com' 
    WHERE customer_id = 1
    """)
    print("Session 1: Update complete!")

def session2_update():
    print("Session 2: Starting update...")
    time.sleep(1)  # Simulate some processing time
    spark.sql("""
    UPDATE lakehouse.tutorial.customers 
    SET email = 'alice.another@example.com' 
    WHERE customer_id = 1
    """)
    print("Session 2: Update complete!")

# Start both sessions
t1 = Thread(target=session1_update)
t2 = Thread(target=session2_update)

t1.start()
t2.start()

t1.join()
t2.join()

# Check final result
print("\nFinal result:")
spark.sql("SELECT * FROM lakehouse.tutorial.customers WHERE customer_id = 1").show()
```

**What happened?**

| Scenario | Result |
|----------|--------|
| **Without ACID** | One update overwrites the other (lost update) |
| **With ACID** | First update to commit succeeds, the other fails |

On this stack, one thread prints `Update complete!` and the other dies with a
traceback ending in an `org.apache.iceberg.exceptions.ValidationException`:

```
# with the default serializable isolation
ValidationException: Found conflicting files that can contain records matching
ref(name="customer_id") == 1: [s3://warehouse/iceberg/tutorial/customers/data/customer_id=1/...parquet]

# with write.update.isolation-level = 'snapshot' (set above)
ValidationException: Missing required files to delete:
s3://warehouse/iceberg/tutorial/customers/data/customer_id=1/...parquet
```

Which thread wins varies from run to run. The final row shows the winner's email,
never a mix of both.

### Understanding the Conflict Resolution

When Polaris detects a conflict:

```
┌──────────────────────────────────────────────────────────────────┐
│           Conflict Resolution Flow                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Session 1 reads current snapshot: snap-10             │
│  Step 2: Session 2 reads current snapshot: snap-10             │
│  Step 3: Session 1 creates new metadata: v11.metadata.json     │
│  Step 4: Session 1 commits to Polaris: snap-10 → snap-11       │
│  Step 5: Polaris updates: "Current snapshot is now snap-11"    │
│  Step 6: Session 2 creates new metadata: v11.metadata.json     │
│  Step 7: Session 2 tries to commit: snap-10 → snap-11          │
│  Step 8: Polaris checks: "Is current still snap-10?"           │
│  Step 9: Polaris says: "No! Current is snap-11 now!"           │
│  Step 10: Session 2's commit FAILS (conflict detected)         │
│                                                                  │
│  Result: Session 1 succeeds, Session 2 must retry              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

Polaris rejects the stale commit with HTTP 409 (`CommitFailedException`). Iceberg
then retries **automatically** (up to `commit.retry.num-retries`, default 4): it
re-reads the new current snapshot and checks whether the other writer's changes
overlap with its own. Non-overlapping changes, such as two appends, are
re-applied and commit without you noticing. Overlapping changes, like two
`UPDATE`s of `customer_id = 1`, fail the check, and Spark raises the
`ValidationException` you saw above. Your code then decides whether to retry.

### Handling Conflicts in Your Code

```python
def safe_update(session_id, customer_id, new_email):
    """Attempt update with conflict handling"""
    try:
        spark.sql(f"""
        UPDATE lakehouse.tutorial.customers 
        SET email = '{new_email}' 
        WHERE customer_id = {customer_id}
        """)
        print(f"Session {session_id}: Update succeeded!")
        return True
    except Exception as e:
        print(f"Session {session_id}: Update failed - {str(e)}")
        return False

# Try multiple times with backoff
def retry_with_backoff(attempts=3):
    for attempt in range(attempts):
        success = safe_update(attempt, 1, f"alice.attempt{attempt}@example.com")
        if success:
            break
        print(f"Retrying in {attempt * 1} seconds...")
        time.sleep(attempt)

retry_with_backoff()
```

---

## 📊 Part 5: Verifying ACID Guarantees

### Test 1: Atomicity

```python
# Test that all-or-nothing works
try:
    spark.sql("""
    INSERT INTO lakehouse.tutorial.customers VALUES
        (999, 'Test User', 'test@example.com', TIMESTAMP '2024-01-01 00:00:00'),
        (1000, 'Another User', 'another@example.com', TIMESTAMP '2024-01-02 00:00:00')
    """)
    
    # Verify both were inserted
    result = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers WHERE customer_id IN (999, 1000)")
    count = result.collect()[0][0]
    print(f"Rows with IDs 999 and 1000: {count}")
    
    if count == 2:
        print("✅ Atomicity test PASSED: Both rows inserted")
    else:
        print("❌ Atomicity test FAILED: Only some rows inserted")
        
except Exception as e:
    print(f"❌ Atomicity test: Exception - {e}")
```

That shows the "all" half. For the "nothing" half, make a 100-row insert fail
part-way through. Some tasks will already have written Parquet files when row 150
blows up, but no snapshot is committed, so readers never see them:

```python
snapshots_before = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers.snapshots").collect()[0][0]
rows_before = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers").collect()[0][0]

try:
    spark.sql("""
    INSERT INTO lakehouse.tutorial.customers
    SELECT CAST(id AS INT), 'Bulk User', 'bulk@example.com',
           CASE WHEN id = 150 THEN CAST(raise_error('boom at row 150') AS TIMESTAMP)
                ELSE TIMESTAMP '2024-03-01 00:00:00' END
    FROM range(100, 200)
    """)
except Exception as e:
    print("Insert failed as expected:", str(e)[:80])

snapshots_after = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers.snapshots").collect()[0][0]
rows_after = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers").collect()[0][0]

if (snapshots_before, rows_before) == (snapshots_after, rows_after):
    print("✅ Atomicity test PASSED: failed insert left no rows and no snapshot")
else:
    print("❌ Atomicity test FAILED: partial data is visible")
```

### Test 2: Consistency

Iceberg enforces the table **schema**: column count, column types and
`NOT NULL`. A write that breaks it is rejected before anything is committed:

```python
# Test that the schema is enforced
try:
    # customer_id is INT -- a string must be rejected, not silently stored
    spark.sql("""
    INSERT INTO lakehouse.tutorial.customers VALUES
        ('not-a-number', 'Bad Row', 'bad@example.com', TIMESTAMP '2024-01-01 00:00:00')
    """)
    print("❌ Consistency test FAILED: wrong type accepted")
except Exception as e:
    print(f"✅ Consistency test PASSED: schema enforced - {str(e)[:120]}")
```

> **Iceberg has no primary keys or unique constraints.** Inserting a second row
> with `customer_id = 1` succeeds, and the table then has two rows with that id.
> To avoid duplicates, write with `MERGE INTO ... ON t.customer_id = s.customer_id`
> instead of a plain `INSERT`:
>
> ```python
> spark.sql("""
> MERGE INTO lakehouse.tutorial.customers t
> USING (SELECT 1 AS customer_id, 'Alice Smith' AS name, 'alice@example.com' AS email,
>               TIMESTAMP '2024-01-01 10:00:00' AS created_at) s
> ON t.customer_id = s.customer_id
> WHEN MATCHED THEN UPDATE SET *
> WHEN NOT MATCHED THEN INSERT *
> """)
> ```

### Test 3: Isolation

```python
# Test concurrent operations don't interfere
from concurrent.futures import ThreadPoolExecutor

def read_operation():
    result = spark.sql("SELECT * FROM lakehouse.tutorial.customers WHERE customer_id = 1")
    return result.collect()

def write_operation():
    spark.sql("""
    UPDATE lakehouse.tutorial.customers 
    SET name = 'Updated Name' 
    WHERE customer_id = 1
    """)

# Run read and write concurrently
with ThreadPoolExecutor(max_workers=2) as executor:
    read_future = executor.submit(read_operation)
    write_future = executor.submit(write_operation)
    
    read_result = read_future.result()
    write_result = write_future.result()
    
    # Check isolation
    if len(read_result) > 0:
        print(f"✅ Isolation test PASSED: Read completed during write")
    else:
        print("❌ Isolation test FAILED")
```

### Test 4: Durability

```python
# Test that committed data persists
print("Before restart:")
initial_count = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers").collect()[0][0]
print(f"Row count: {initial_count}")

# Simulate restart (just reinitialize Spark)
spark.stop()

# Create a new session (same canonical config as Step 1)
from pyspark.sql import SparkSession

# The object-store key is generated at first boot; read it from the shared volume.
creds = dict(line.strip().split("=", 1)
             for line in open("/creds/garage-credentials.env") if "=" in line)

spark = (
    SparkSession.builder.appName("iceberg-tutorial")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Catalog: Polaris, over the Iceberg REST protocol
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog")
    .config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")   # Polaris catalog NAME
    .config("spark.sql.catalog.lakehouse.credential", "root:root")
    .config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
    # Storage: Garage, over the S3 protocol
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://objectstore:3900")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", creds["AWS_ACCESS_KEY_ID"])
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", creds["AWS_SECRET_ACCESS_KEY"])
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    # Polaris does not grant the root principal metrics reporting; skip it.
    .config("spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled", "false")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)

print("After restart:")
new_count = spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers").collect()[0][0]
print(f"Row count: {new_count}")

if initial_count == new_count:
    print("✅ Durability test PASSED: Data persisted through restart")
else:
    print("❌ Durability test FAILED: Data lost")
```

---

## 🎯 Part 6: Hands-on Exercises

### Exercise 1: Basic CRUD Operations

Create a table and perform basic operations:

```python
# 1. Create a table
# TODO: Create a 'products' table with columns: product_id, name, price

# 2. Insert data
# TODO: Insert at least 5 products

# 3. Read data
# TODO: Query all products

# 4. Update data
# TODO: Update price of one product

# 5. Delete data
# TODO: Delete one product

# 6. Verify final state
# TODO: Count rows and verify
```

### Exercise 2: Concurrent Updates

Simulate concurrent updates to understand conflict handling:

```python
# TODO: Create a function that updates a customer's name
# TODO: Run it twice concurrently
# TODO: Observe the result and document what happened
# TODO: Try to understand why you got that result
```

### Exercise 3: Transaction Rollback

Simulate what happens when a transaction fails:

Spark SQL has no `BEGIN`/`COMMIT`: every statement is its own transaction.
So "rollback" here means a single statement that fails part-way (see the
`raise_error` trick in Test 1).

```python
# TODO: Record the current snapshot_id from lakehouse.tutorial.customers.snapshots
# TODO: Run an UPDATE or MERGE that fails part-way through
# TODO: Verify no new snapshot was created and the data is unchanged
# TODO: Document the behavior
```

---

## 📝 Summary

| Concept | Key Takeaway |
|---------|--------------|
| **Atomicity** | All operations in a transaction succeed or fail together |
| **Consistency** | Database remains in valid state (schema enforced; no primary-key constraints) |
| **Isolation** | Concurrent transactions don't interfere (Polaris handles this) |
| **Durability** | Committed data persists even after failures |

| Component | Role in ACID |
|-----------|--------------|
| **Spark** | Performs the actual data operations |
| **Polaris** | Enforces ACID by atomically swapping the metadata pointer only if the table has not changed (optimistic concurrency) |
| **Garage** | Stores data and metadata files durably (S3 API) |
| **Iceberg** | Provides the table format with ACID capabilities |

---

## 🚀 Challenge Questions

1. **What happens if two users update the same row at the exact same millisecond?**
2. **How does Polaris know which snapshot is current?**
3. **Can you read data while a transaction is being committed?**
4. **What happens to uncommitted changes if Spark crashes?**
5. **How does Iceberg handle network partitions during commits?**

---

## 📚 Additional Reading

- [Iceberg ACID Transactions Specification](https://iceberg.apache.org/spec/)
- [Polaris Documentation](https://polaris.apache.org/docs/)
- [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---

**You've completed the Spark ACID Transactions lab! Next, try the Trino and Polaris implementations to see how each tool handles ACID differently.**