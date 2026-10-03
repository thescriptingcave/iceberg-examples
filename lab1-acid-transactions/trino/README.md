# Lab 1: ACID Transactions (Trino Implementation)

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand ACID properties** through Trino's SQL interface
2. **Create Iceberg tables** using Trino SQL
3. **Perform transactions** with SQL commands
4. **Handle concurrent transactions** and understand isolation
5. **Verify ACID guarantees** through SQL queries
6. **Compare Trino with Spark** for transaction handling

---

## 📚 Part 1: Understanding ACID Through Trino SQL

### ACID Properties in SQL

Trino's Iceberg connector brings ACID guarantees to SQL queries:

| ACID Property | SQL Equivalent | Trino Implementation |
|--------------|----------------|---------------------|
| **Atomicity** | A single SQL statement (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) | All-or-nothing: one new snapshot, or nothing |
| **Consistency** | Column types, `NOT NULL` | Type and `NOT NULL` checks before commit (no foreign keys or check constraints) |
| **Isolation** | Transaction isolation levels | Snapshot isolation: every query reads one snapshot |
| **Durability** | Committed transactions persist | Data and metadata files written to the object store (Garage) before Polaris commits the new snapshot |

### Trino Transaction Syntax

```sql
-- Explicit transaction (Trino uses START TRANSACTION, not BEGIN)
START TRANSACTION;
-- Your SQL statements here
COMMIT;      -- or ROLLBACK;
```

> **Important -- the Iceberg connector only writes in autocommit mode.**
> Inside `START TRANSACTION ... COMMIT` you can only *read* Iceberg tables. Any
> `INSERT`, `UPDATE`, `DELETE` or `MERGE` there fails with
> `Catalog only supports writes using autocommit: iceberg`.
> So in Trino **each write statement is its own transaction**: it commits
> atomically when it finishes. To change several rows in different ways
> atomically, put the changes into a single `MERGE` statement (Part 3).
> Trino also has no `BEGIN` keyword and no `SAVEPOINT`.

### How Trino Handles ACID

```
┌──────────────────────────────────────────────────────────────────┐
│           Trino ACID Transaction Flow                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  User: UPDATE customers SET email = 'dave@example.com'         │
│        WHERE customer_id = 4                                    │
│  └── Trino Coordinator plans the statement (autocommit)        │
│                                                                  │
│  Workers execute the UPDATE                                     │
│      ├── Read the current snapshot (from Polaris)              │
│      ├── Write new data files + delete files to Garage         │
│      └── Nothing is visible to other readers yet               │
│                                                                  │
│  Statement finishes → Coordinator commits to Polaris           │
│      ├── Writes new metadata files to Garage                   │
│      ├── Asks Polaris: "Is current still snap-1? Then snap-2"  │
│      ├── Polaris confirms → Commits metadata                   │
│      └── Returns: "UPDATE: 1 row"                               │
│                                                                  │
│  If the statement fails (error, conflict, cancel):              │
│  └── No new snapshot is committed                              │
│      └── Files already written are orphans, never read         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Transaction Isolation in Trino

| Isolation Level | Trino Behavior |
|-----------------|----------------|
Trino accepts all four `ISOLATION LEVEL` clauses on `START TRANSACTION`, but for
Iceberg tables the behaviour is the same whichever you pick:

| Situation | Trino Behavior |
|-----------------|----------------|
| **Single query** | Reads one committed snapshot; never sees uncommitted data |
| **Explicit (read-only) transaction** | The first read of a table pins its snapshot; later reads in the same transaction see the same data (snapshot isolation) |
| **Concurrent writes** | Optimistic concurrency: the second writer to commit is checked for conflicts and fails if it touched the same data |

---

## 🛠️ Part 2: Creating Your Iceberg Table with Trino

### Step 1: Connect to Trino

```bash
# Connect to Trino CLI
docker exec -it iceberg-trino trino --catalog iceberg --schema tutorial
```

You should see:

```
trino> 
```

This is your Trino prompt!

The Trino catalog is called `iceberg` and the lab namespace is `tutorial`, so
the full table name is `iceberg.tutorial.customers` (the same table Spark calls
`lakehouse.tutorial.customers`). See
[`common/connection-reference.md`](../../common/connection-reference.md) for all
endpoints and names.

### Step 2: Create a Namespace

In Trino a namespace is called a **schema**. Lab 0 already created `tutorial`;
this is a no-op if it exists:

```sql
-- Create the schema (namespace) if it doesn't exist
CREATE SCHEMA IF NOT EXISTS iceberg.tutorial;
```

> **Already ran Lab 0?** Its init script creates `tutorial.customers` with the
> same three rows. Check with `SHOW TABLES;` -- if `customers` is listed, skip
> Steps 3 and 5 (the only visible difference is that `created_at` shows as
> `timestamp(6) with time zone`, because Spark's `TIMESTAMP` is zone-aware).

### Step 3: Create an Iceberg Table

```sql
-- Create an Iceberg table
CREATE TABLE tutorial.customers (
    customer_id INTEGER,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP(6)
) WITH (
    partitioning = ARRAY['customer_id']
);
```

**Understanding the syntax:**

| Component | Purpose |
|-----------|---------|
| `INTEGER` | Trino's equivalent of INT |
| `VARCHAR(100)` | Variable character string with max length |
| `TIMESTAMP(6)` | Timestamp with 6 digits of precision |
| `WITH (...)` | Table properties |
| `partitioning` | How data is partitioned (we'll cover this in Lab 3) |

### Step 4: Verify Table Creation

```sql
-- Show tables
SHOW TABLES;

-- Describe table schema
DESCRIBE tutorial.customers;
```

You should see:

```
   Column    |     Type     | Extra | Comment
-------------+--------------+-------+---------
 customer_id | integer      |       |
 name        | varchar      |       |
 email       | varchar      |       |
 created_at  | timestamp(6) |       |
(4 rows)
```

Note that `name` and `email` are plain `varchar`: Iceberg has no length-limited
string type, so the `(100)` is not stored.

### Step 5: Insert Initial Data

```sql
-- Insert data
INSERT INTO tutorial.customers (customer_id, name, email, created_at)
VALUES
    (1, 'Alice Smith', 'alice@example.com', TIMESTAMP '2024-01-01 10:00:00'),
    (2, 'Bob Johnson', 'bob@example.com', TIMESTAMP '2024-01-02 11:00:00'),
    (3, 'Charlie Brown', 'charlie@example.com', TIMESTAMP '2024-01-03 12:00:00');

-- Verify insertion
SELECT COUNT(*) AS row_count FROM tutorial.customers;
```

You should see:

```
 row_count 
-----------
         3
(1 row)
```

### Step 6: Query the Data

```sql
-- Simple SELECT
SELECT * FROM tutorial.customers;

-- With WHERE clause
SELECT name, email FROM tutorial.customers WHERE customer_id = 1;

-- With ORDER BY
SELECT * FROM tutorial.customers ORDER BY created_at DESC;
```

---

## 🔍 Part 3: Understanding Transactions in Trino

### Transaction Flow

Let's trace through a single write statement (remember: with Iceberg, every
write statement is its own transaction):

```
┌──────────────────────────────────────────────────────────────────┐
│           Trino Transaction Flow                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. STATEMENT STARTS (autocommit)                                │
│     └── Trino creates a transaction for this one statement      │
│                                                                  │
│  2. READ CURRENT SNAPSHOT                                       │
│     └── Trino asks Polaris: "What's current snapshot?"        │
│     └── Polaris responds: "snap-10"                            │
│                                                                  │
│  3. PROCESS THE STATEMENT, e.g. a MERGE that                    │
│     ├── inserts customer 4                                     │
│     │   └── Trino writes a new data file                      │
│     ├── updates customer 1's email                             │
│     │   └── Trino writes the new row + a delete file          │
│     └── deletes customer 3                                     │
│         └── Trino writes a position delete file               │
│                                                                  │
│  4. PREPARE METADATA                                            │
│     └── Trino creates metadata with new snapshot               │
│                                                                  │
│  5. COMMIT                                                      │
│     └── Trino sends to Polaris: "Update snap-10 → snap-11"   │
│     └── Polaris validates and commits                          │
│     └── Trino prints: "MERGE: 3 rows"                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Several Changes, One Atomic Commit: MERGE

Because you cannot group several write statements in `START TRANSACTION`,
`MERGE` is how you insert, update and delete in one all-or-nothing commit:

```sql
MERGE INTO tutorial.customers t
USING (VALUES
    (1, 'Alice Smith', 'alice@new.example.com'),   -- exists     -> update
    (3, NULL, NULL),                               -- exists     -> delete
    (4, 'Dave Wilson', 'dave@example.com')         -- new        -> insert
) AS s(customer_id, name, email)
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.name IS NULL THEN DELETE
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email
WHEN NOT MATCHED THEN INSERT (customer_id, name, email, created_at)
    VALUES (s.customer_id, s.name, s.email, current_timestamp);

-- One statement, one new snapshot
SELECT customer_id, name, email FROM tutorial.customers ORDER BY customer_id;
SELECT snapshot_id, operation, summary FROM tutorial."customers$snapshots"
ORDER BY committed_at DESC LIMIT 1;
```

The `MERGE` reports `MERGE: 3 rows`, and the latest snapshot is a single
`overwrite` whose summary counts the added records and the position deletes
together.

### Snapshot Isolation

Reads in an explicit transaction see one snapshot per table. Open **two**
terminals running `docker exec -it iceberg-trino trino --catalog iceberg --schema tutorial`:

```sql
-- Terminal 1: start a (read) transaction
START TRANSACTION;

-- Read current data
SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- Returns: customer_id=1, name='Alice Smith', email='alice@new.example.com'

-- Terminal 2 (autocommit): someone updates Alice's email
-- UPDATE tutorial.customers SET email = 'alice.iso@example.com' WHERE customer_id = 1;

-- Terminal 1: read again in your transaction
SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- Still returns email='alice@new.example.com'
-- (snapshot isolation: the first read pinned the snapshot)

-- End your transaction
COMMIT;

-- Now read outside the transaction
SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- Returns email='alice.iso@example.com'
-- (new snapshot after the other transaction committed)
```

### Detecting Concurrent Updates

When two statements update the same rows at the same time:

```
┌──────────────────────────────────────────────────────────────────┐
│           Concurrent Update Scenario                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Session 1                              Session 2                │
│  ───────────                            ───────────              │
│  UPDATE customers SET                  UPDATE customers SET      │
│    name = 'Alice Session 1'            name = 'Alice Session 2' │
│  WHERE customer_id = 1                 WHERE customer_id = 1     │
│  -- both start from snap-10                                     │
│                                                                  │
│  Commit                                 Commit                   │
│  └── Polaris checks:                   └── Polaris checks:      │
│      "Is current snap-10?"               "Is current snap-10?"  │
│      └── YES → Commit!                   └── NO → snap-11       │
│                                          Trino re-validates:     │
│                                          snap-11 touched the     │
│                                          same rows → FAIL!       │
│                                                                  │
│  Result: the first to commit succeeds, the other fails with:    │
│  "Failed to commit the transaction during write: Found          │
│   conflicting files that can contain records matching ..."     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

A conflict is only raised when the other commit could contain rows the
statement matched. Two concurrent updates to *different* partitions (say
`customer_id = 1` and `customer_id = 2`) both succeed.

### Handling Commit Conflicts

```sql
-- Each UPDATE is its own transaction and commits when it finishes
UPDATE tutorial.customers
SET email = 'alice.updated@example.com'
WHERE customer_id = 1;

-- If another writer committed a conflicting change first, it fails with
-- "Failed to commit the transaction during write: Found conflicting files ..."
-- Nothing was committed, so it is safe to simply run the statement again.
```

---

## 🚨 Part 4: Handling Concurrent Transactions

### Scenario: Two Trino Sessions

Let's simulate two sessions updating the same row. Paste each statement in a
different terminal and press Enter in both at (nearly) the same moment.

**Session 1:**
```sql
UPDATE tutorial.customers SET name = 'Alice Session 1' WHERE customer_id = 1;
```

**Session 2:**
```sql
UPDATE tutorial.customers SET name = 'Alice Session 2' WHERE customer_id = 1;
```

**Expected Result:**
- Whichever session commits first succeeds (`UPDATE: 1 row`)
- The other gets the `Found conflicting files` error
- Alice's name is the winner's value

It is hard to start two statements by hand within the same second. From a
shell on your laptop this runs them truly in parallel:

```bash
docker exec iceberg-trino trino --execute "UPDATE iceberg.tutorial.customers SET name = 'Alice Session 1' WHERE customer_id = 1" &
docker exec iceberg-trino trino --execute "UPDATE iceberg.tutorial.customers SET name = 'Alice Session 2' WHERE customer_id = 1" &
wait
```

### Implementing Retry Logic

Trino SQL has no loops, so retries live in the client (a script, an
orchestrator, or you re-running the statement):

```sql
-- Session 1
UPDATE tutorial.customers SET name = 'Alice Final' WHERE customer_id = 1;
```

```sql
-- Session 2 (run after Session 1 commits)
UPDATE tutorial.customers SET name = 'Alice Final' WHERE customer_id = 1;
```

In this case, both succeed: Session 2 starts from the snapshot Session 1
created, so there is nothing to conflict with.

But if they run at the same time and touch the same rows:

```sql
-- Session 1
UPDATE tutorial.customers SET name = 'Alice 1' WHERE customer_id = 1;
```

```sql
-- Session 2 (run in parallel with Session 1)
UPDATE tutorial.customers SET name = 'Alice 2' WHERE customer_id = 1;
```

One of them fails with a commit conflict error; re-run it and it succeeds.
Note that the conflict is detected even though both set the *same column* --
Iceberg checks for overlapping data files, not for overlapping values.

---

## 📊 Part 5: Verifying ACID Guarantees

### Test 1: Atomicity

```sql
-- One multi-row INSERT = one transaction = one snapshot
INSERT INTO tutorial.customers (customer_id, name, email, created_at)
VALUES
    (998, 'Test 1', 'test1@example.com', TIMESTAMP '2024-01-01'),
    (999, 'Test 2', 'test2@example.com', TIMESTAMP '2024-01-01');

-- Both rows arrived together
SELECT COUNT(*) AS inserted_count FROM tutorial.customers WHERE customer_id IN (998, 999);

-- ...in a single snapshot
SELECT snapshot_id, operation, summary['added-records'] AS added_records
FROM tutorial."customers$snapshots" ORDER BY committed_at DESC LIMIT 1;

-- Writes inside an explicit transaction are rejected outright:
START TRANSACTION;
INSERT INTO tutorial.customers VALUES (997, 'Test 0', 'test0@example.com', TIMESTAMP '2024-01-01');
-- Query failed: Catalog only supports writes using autocommit: iceberg
ROLLBACK;
```

### Test 2: Consistency

```sql
-- Try to insert a NULL customer_id
INSERT INTO tutorial.customers (customer_id, name, email, created_at)
VALUES (NULL, 'Test', 'test@example.com', TIMESTAMP '2024-01-01');

-- This SUCCEEDS: the column was not declared NOT NULL, so there is
-- no constraint to enforce. Clean up:
DELETE FROM tutorial.customers WHERE customer_id IS NULL;

-- Declare the constraint and try again
CREATE TABLE tutorial.customers_strict (
    customer_id INTEGER NOT NULL,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP(6)
);

INSERT INTO tutorial.customers_strict
VALUES (NULL, 'Test', 'test@example.com', TIMESTAMP '2024-01-01');
-- Query failed: NULL value not allowed for NOT NULL column: customer_id

-- Type checks also happen before anything is written
INSERT INTO tutorial.customers_strict
VALUES (1, 'Wrong type', 'x', 'not-a-timestamp');
-- Query failed: Insert query has mismatched column types ...

DROP TABLE tutorial.customers_strict;
```

Iceberg has no primary keys, foreign keys or check constraints: inserting a
second row with `customer_id = 1` is allowed.

### Test 3: Isolation

```sql
-- Session 1
START TRANSACTION;
SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- Note the email value
-- Don't commit yet

-- Session 2 (in parallel, autocommit)
UPDATE tutorial.customers SET email = 'alice.isolation@example.com' WHERE customer_id = 1;

-- Session 1 (continue)
SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- Should still see the old email (snapshot isolation)
COMMIT;
```

### Test 4: Durability

```sql
-- Check table metadata: one row per committed snapshot
SELECT snapshot_id, parent_id, operation, committed_at
FROM tutorial."customers$snapshots" ORDER BY committed_at;

-- Check manifest files
SELECT path, added_data_files_count, added_rows_count FROM tutorial."customers$manifests";

-- Read an older snapshot (use a snapshot_id from the query above)
SELECT * FROM tutorial.customers FOR VERSION AS OF <snapshot_id>;
```

The data and metadata files live in the object store and the current-snapshot
pointer lives in Polaris (backed by PostgreSQL). Restart Trino
(`docker compose restart trino`) and query again -- the data is still there.

---

## 🎯 Part 6: Hands-on Exercises

### Exercise 1: Basic CRUD, One Transaction per Statement

```sql
-- 1. Create a 'products' table
-- TODO: Create tutorial.products with product_id, name, price

-- 2. Insert 5 products (one INSERT statement)
-- TODO: INSERT statement

-- 3. Update one product's price
-- TODO: UPDATE statement

-- 4. Delete one product
-- TODO: DELETE statement

-- 5. Count the snapshots: how many transactions did you run? (CREATE TABLE counts too)
-- TODO: SELECT ... FROM tutorial."products$snapshots"

-- 6. Verify the final state
-- TODO: SELECT COUNT(*) and SELECT * FROM tutorial.products

-- 7. Bonus: do steps 3 and 4 again as a single MERGE, and check
--    that it produced only one snapshot
```

### Exercise 2: Concurrent Updates

```sql
-- Session 1
-- TODO: UPDATE a customer's email

-- Session 2 (run concurrently, e.g. with the `&` shell trick from Part 4)
-- TODO: UPDATE the same customer's email to a DIFFERENT value
-- TODO: Observe which session fails
-- TODO: Document the error
-- TODO: Repeat with two DIFFERENT customers. Does either fail?
```

### Exercise 3: Undoing a Change

Since writes commit immediately, `ROLLBACK` cannot undo them. Instead you roll
the table back to an earlier snapshot:

```sql
-- 1. Note the current snapshot
-- SELECT snapshot_id FROM tutorial."customers$snapshots" ORDER BY committed_at DESC LIMIT 1;

-- 2. Make a change (commits immediately)
-- UPDATE tutorial.customers SET email = 'temp@example.com' WHERE customer_id = 1;

-- 3. Verify the change is visible, and that the old snapshot still has the old value
-- SELECT * FROM tutorial.customers WHERE customer_id = 1;
-- SELECT * FROM tutorial.customers FOR VERSION AS OF <snapshot_id> WHERE customer_id = 1;

-- 4. Roll the table back to the snapshot from step 1
-- ALTER TABLE tutorial.customers EXECUTE rollback_to_snapshot(<snapshot_id>);

-- 5. Verify the change was reverted
-- SELECT * FROM tutorial.customers WHERE customer_id = 1;
```

---

## 📝 Summary

| Concept | Trino Implementation |
|---------|---------------------|
| **Atomicity** | Each write statement is all-or-nothing |
| **Consistency** | Types and `NOT NULL` enforced before commit |
| **Isolation** | Snapshot isolation; conflicting concurrent writes fail |
| **Durability** | Files persist in the object store, snapshot pointer in Polaris |

| Trino Feature | Purpose |
|---------------|---------|
| `START TRANSACTION` | Start a transaction (read-only for Iceberg) |
| `COMMIT` | Commit a transaction |
| `ROLLBACK` | End a transaction without committing |
| `MERGE` | Insert, update and delete atomically in one statement |
| `FOR VERSION AS OF` | Read an older snapshot |
| `ALTER TABLE ... EXECUTE rollback_to_snapshot` | Undo committed changes |
| `SHOW TABLES` | List tables in current namespace |
| `DESCRIBE` | Show table schema |

---

## 🚀 Challenge Questions

1. **What happens if you COMMIT twice?**
2. **Can you read uncommitted data from another session?**
3. **How does Trino detect concurrent updates?**
4. **What's the difference between READ COMMITTED and SERIALIZABLE?**
5. **Can you perform transactions across multiple catalogs?**

---

## 📚 Additional Reading

- [Trino Iceberg Connector Documentation](https://trino.io/docs/current/connector/iceberg.html)
- [Trino SQL Syntax](https://trino.io/docs/current/sql.html)
- [Iceberg ACID Transactions](https://iceberg.apache.org/spec/)

---

**You've completed the Trino ACID Transactions lab! Compare your findings with the Spark implementation to understand how different engines handle ACID.**