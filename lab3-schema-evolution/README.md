# Lab 3: Schema Evolution

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand schema evolution** in Iceberg tables
2. **Add columns** to existing tables
3. **Remove columns** from tables
4. **Update column types** and handle compatibility
5. **Rename columns** without breaking queries
6. **Understand backward and forward compatibility**

## 📚 Part 1: Understanding Schema Evolution

### What is Schema Evolution?

Schema evolution is the ability to modify a table's schema over time without breaking existing queries or data.

```
┌──────────────────────────────────────────────────────────────────┐
│              Schema Evolution Timeline                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │  Version 1   │────▶│  Version 2   │────▶│  Version 3   │    │
│  │              │     │              │     │              │    │
│  │ customer_id  │     │ customer_id  │     │ customer_id  │    │
│  │ name         │     │ name         │     │ name         │    │
│  │ email        │     │ email        │     │ email        │    │
│  │              │     │                │     │ phone        │    │
│  │              │     │                │     │ address      │    │
│  └──────────────┘     └──────────────┘     └──────────────┘    │
│       │                      │                      │            │
│       └──────────────────────┴──────────────────────┘            │
│                              │                                    │
│                    Schema evolution adds columns                 │
│                    without breaking existing queries             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Why Schema Evolution Matters

| Scenario | Without Schema Evolution | With Schema Evolution |
|----------|-------------------------|----------------------|
| **Add column** | Must recreate table | Add column, existing queries still work |
| **Remove column** | Must recreate table | Drop column, queries that don't use it still work |
| **Change type** | Data loss or errors | Safe widening in place (e.g. INT → BIGINT); other changes need explicit conversion |
| **Rename column** | Must rewrite data files | Rename is metadata only (columns are tracked by ID) |

### Backward vs Forward Compatibility

```
┌──────────────────────────────────────────────────────────────────┐
│         Backward vs Forward Compatibility                       │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐                         │
│  │  Writer V1   │     │  Writer V2   │                         │
│  │              │     │              │                         │
│  │ customer_id  │     │ customer_id  │                         │
│  │ name         │     │ name         │                         │
│  │ email        │     │ email        │                         │
│  │              │     │ phone        │ ← New column            │
│  └──────────────┘     └──────────────┘                         │
│       │                      │                                  │
│       │                      └──────────────┐                  │
│       │                                     │                  │
│       ▼                                     ▼                  │
│  ┌──────────────┐                    ┌──────────────┐          │
│  │  Reader V1   │                    │  Reader V2   │          │
│  │              │                    │              │          │
│  │ customer_id  │                    │ customer_id  │          │
│  │ name         │                    │ name         │          │
│  │ email        │                    │ email        │          │
│  │ phone: NULL  │ ← Backward         │ phone        │          │
│  │ (safe)       │    compatibility   │ (safe)       │          │
│  └──────────────┘                    └──────────────┘          │
│       │                                                      │  │
│       └──────────────────────────────────────────────────────┘  │
│                              │                                    │
│                    Backward compatibility:                       │
│                    New reader can read old data                  │
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐                         │
│  │  Writer V1   │     │  Writer V2   │                         │
│  │              │     │              │                         │
│  │ customer_id  │     │ customer_id  │                         │
│  │ name         │     │ name         │                         │
│  │ email        │     │ email        │                         │
│  └──────────────┘     └──────────────┘                         │
│       │                      │                                  │
│       └──────────────┐       │                                  │
│                      │       │                                  │
│                      ▼       ▼                                  │
│  ┌──────────────┐     ┌──────────────┐                         │
│  │  Reader V1   │     │  Reader V2   │                         │
│  │              │     │              │                         │
│  │ customer_id  │     │ customer_id  │                         │
│  │ name         │     │ name         │                         │
│  │ email        │     │ email        │                         │
│  │ phone: NULL  │     │ phone: NULL  │ ← Forward               │
│  └──────────────┘     └──────────────┘    compatibility        │
│                                                                  │
│                    Forward compatibility:                        │
│                    Old reader can read new data                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Iceberg Schema Evolution Rules

| Operation | Backward Compatible | Forward Compatible | Notes |
|-----------|-------------------|-------------------|-------|
| **Add column** | ✅ Yes | ✅ Yes | New columns read as NULL for old data |
| **Remove column** | ✅ Yes | ⚠️ Queries using the column break | Old files keep the values, but they are no longer read |
| **Update type** | ✅ Safe widening only | ✅ Safe widening only | INT → BIGINT, FLOAT → DOUBLE, wider DECIMAL; anything else needs explicit conversion |
| **Rename column** | ✅ Yes | ⚠️ Queries using the old name break | Only metadata changes |

---

## 🛠️ Part 2: Adding Columns with Spark

The examples below assume a `spark` session created with the canonical snippet in
[common/connection-reference.md](../common/connection-reference.md#spark-in-a-jupyter-notebook).

> This lab works on its own table, `lakehouse.tutorial.customers_evo`, so that it
> does not collide with the `customers` table from Lab 0 (which already has a
> `created_at` column) or damage the history you built in Lab 2.

### Method 1: Using ALTER TABLE

```python
# Create initial table
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.tutorial.customers_evo (
    customer_id INT,
    name STRING,
    email STRING
) USING ICEBERG
""")

# Insert initial data
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_evo VALUES
    (1, 'Alice Smith', 'alice@example.com'),
    (2, 'Bob Johnson', 'bob@example.com')
""")

# Add a new column
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_evo ADD COLUMN phone STRING
""")

print("✅ Column 'phone' added!")

# Verify the schema
spark.sql("DESCRIBE lakehouse.tutorial.customers_evo").show()
```

You can also choose where the new column goes and give it a comment:
`ALTER TABLE ... ADD COLUMN signup_channel STRING COMMENT 'web, app or store' AFTER email`
(or `FIRST`).

### Method 2: Adding a Column Without a Default Value

```python
# Add a timestamp column
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_evo ADD COLUMN created_at TIMESTAMP
""")

# Existing rows will have NULL for the new column

# Insert a new row -- use a TIMESTAMP literal; Spark will not cast a plain
# string into a TIMESTAMP column on insert
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_evo VALUES
    (3, 'Charlie Brown', 'charlie@example.com', '555-1234', TIMESTAMP '2024-01-01 10:00:00')
""")

# Query and see NULLs for the old rows
spark.sql("""
SELECT customer_id, name, phone, created_at FROM lakehouse.tutorial.customers_evo
""").show()
```

> **Column defaults are not supported on this stack.** With Spark 3.5 and
> Iceberg 1.9.1 (format version 2), `ADD COLUMN ... DEFAULT <value>` is
> accepted without error, but the default is silently discarded: it does not
> appear in `SHOW CREATE TABLE`, old rows read as NULL, `INSERT ... DEFAULT`
> writes NULL, and an `INSERT` that leaves the column out fails. Default values
> are an Iceberg format v3 feature; don't rely on them here.

### Querying Evolved Schema

```python
# Query only existing columns (works perfectly)
spark.sql("""
SELECT customer_id, name, email FROM lakehouse.tutorial.customers_evo
""").show()

# Query with new column
spark.sql("""
SELECT customer_id, name, phone FROM lakehouse.tutorial.customers_evo
""").show()

# Notice: phone column shows NULL for old data
```

Trino sees the evolved schema immediately:
`docker exec iceberg-trino trino --execute "SELECT * FROM iceberg.tutorial.customers_evo"`.

---

## 🔄 Part 3: Removing Columns

### Method 1: ALTER TABLE DROP COLUMN

Iceberg drops a column by removing it from the current schema. This is a
metadata-only change: no data files are rewritten. Old files still contain the
values, but readers no longer see them.

```python
# Add a column we will drop again
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_evo ADD COLUMN signup_channel STRING
""")

# Drop it
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_evo DROP COLUMN signup_channel
""")

spark.sql("DESCRIBE lakehouse.tutorial.customers_evo").show()
```

Columns are tracked by ID, not by name. If you later add a column with the same
name, it is a *new* column: it reads as NULL for existing rows, and the dropped
values do not come back. The old values are still visible by time travel to a
snapshot from before the drop (see Lab 2).

### Method 2: Creating a New Table Without the Column

Use this when you want a physically separate copy without the column:

```python
# Create a new table without the unwanted column
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_evo_v2 AS
SELECT customer_id, name, email FROM lakehouse.tutorial.customers_evo
""")

# Verify
spark.sql("DESCRIBE lakehouse.tutorial.customers_evo_v2").show()

# Replace the old table (this also drops phone and created_at -- the copy only
# has the three selected columns). PURGE also deletes the old table's files;
# without PURGE they stay in the bucket. The rename target is written without
# the catalog prefix; with it, Polaris reports "Namespace does not exist".
spark.sql("DROP TABLE lakehouse.tutorial.customers_evo PURGE")
spark.sql("ALTER TABLE lakehouse.tutorial.customers_evo_v2 RENAME TO tutorial.customers_evo")
```

### Method 3: Using DataFrame API

```python
# Read table
df = spark.read.format("iceberg").load("lakehouse.tutorial.customers_evo")

# Select only desired columns
df_selected = df.select("customer_id", "name", "email")

# Write to new table (creates it, or replaces it if it exists)
df_selected.writeTo("lakehouse.tutorial.customers_recovered").createOrReplace()
```

---

## 📊 Part 4: Updating Column Types

### Type Promotion Rules

Iceberg only allows type changes that can never lose data. They are
metadata-only: existing files are read and widened on the fly.

| From Type | To Type | Allowed with `ALTER COLUMN ... TYPE`? | Example |
|-----------|---------|-------------|---------|
| INT | BIGINT | ✅ Yes | 1 → 1L |
| FLOAT | DOUBLE | ✅ Yes | 1.5 → 1.5 |
| DECIMAL(P,S) | DECIMAL(P2,S), P2 > P | ✅ Yes (scale must stay the same) | DECIMAL(10,2) → DECIMAL(12,2) |
| STRING | INT | ❌ No | rewrite with `CAST` (Method 1) |
| BIGINT | INT | ❌ No (narrowing) | rewrite with `CAST` |
| TIMESTAMP | DATE | ❌ No | rewrite with `CAST` |

### Method 1: Creating a New Table with New Type

```python
# Create table with original type
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_num (
    customer_id INT,
    name STRING,
    age STRING  -- Originally string
) USING ICEBERG
""")

# Insert data
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_num VALUES
    (1, 'Alice', '30'),
    (2, 'Bob', '25')
""")

# Create new table with INT type
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_age_int AS
SELECT
    customer_id,
    name,
    CAST(age AS INT) as age  -- Type conversion
FROM lakehouse.tutorial.customers_num
""")

# Verify
spark.sql("DESCRIBE lakehouse.tutorial.customers_age_int").show()
spark.sql("SELECT * FROM lakehouse.tutorial.customers_age_int").show()
```

### Method 2: Using ALTER TABLE (Safe Widening Only)

```python
# Widening INT -> BIGINT is allowed
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_num ALTER COLUMN customer_id TYPE BIGINT
""")
spark.sql("DESCRIBE lakehouse.tutorial.customers_num").show()

# STRING -> INT is not a safe promotion, so it is rejected
try:
    spark.sql("""
    ALTER TABLE lakehouse.tutorial.customers_num ALTER COLUMN age TYPE INT
    """)
except Exception as e:
    print(f"Rejected: {str(e).splitlines()[0]}")
    print("Use the Create New Table approach instead")
```

---

## 🏷️ Part 5: Renaming Columns

### Using ALTER TABLE RENAME

```python
# Rename a column
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_evo RENAME COLUMN email TO email_address
""")

print("✅ Column renamed from 'email' to 'email_address'!")

# Verify the schema
spark.sql("DESCRIBE lakehouse.tutorial.customers_evo").show()

# Query with new column name
spark.sql("""
SELECT customer_id, name, email_address FROM lakehouse.tutorial.customers_evo
""").show()

# Query with old column name (will fail!)
try:
    spark.sql("""
    SELECT customer_id, name, email FROM lakehouse.tutorial.customers_evo
    """).show()
except Exception as e:
    print(f"Old column name 'email' no longer exists!")
    print(f"Error: {e}")
```

### Understanding Metadata-Only Changes

```python
# Every schema change writes a new metadata file -- but no new snapshot
spark.sql("""
SELECT timestamp, file, latest_snapshot_id
FROM lakehouse.tutorial.customers_evo.metadata_log_entries
""").show(truncate=False)

# Time travel to the first snapshot uses the schema of that snapshot,
# so the column is still called 'email' there
first_snapshot_id = spark.sql("""
SELECT snapshot_id FROM lakehouse.tutorial.customers_evo.history
ORDER BY made_current_at LIMIT 1
""").first()["snapshot_id"]

spark.sql(f"""
SELECT * FROM lakehouse.tutorial.customers_evo VERSION AS OF {first_snapshot_id}
""").show()
```

---

## 🧪 Part 6: Backward and Forward Compatibility

### Backward Compatibility Test

```python
# Create table with old schema
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_compat (
    customer_id INT,
    name STRING,
    email STRING
) USING ICEBERG
""")

# Insert data
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_compat VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com')
""")

# Evolve schema (add column)
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers_compat ADD COLUMN phone STRING
""")

# Write new data (with phone)
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_compat VALUES
    (3, 'Charlie', 'charlie@example.com', '555-1234')
""")

# Read with old schema (should work - backward compatible)
print("Reading with old schema (missing 'phone' column):")
df_old = spark.sql("SELECT customer_id, name, email FROM lakehouse.tutorial.customers_compat")
df_old.show()
print("✅ Backward compatible: Old query works!")

# Read with new schema (should work)
print("\nReading with new schema (including 'phone' column):")
df_new = spark.sql("SELECT customer_id, name, email, phone FROM lakehouse.tutorial.customers_compat")
df_new.show()
```

### Forward Compatibility Test

```python
# Read new table with old reader (simulated)

# Create new schema
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_newer (
    customer_id INT,
    name STRING,
    email STRING,
    phone STRING,
    address STRING
) USING ICEBERG
""")

# Insert data
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_newer VALUES
    (1, 'Alice', 'alice@example.com', '555-1234', '123 Main St'),
    (2, 'Bob', 'bob@example.com', '555-5678', '456 Oak Ave')
""")

# Read with select only old columns (should work - forward compatible)
print("Reading with old query (only customer_id, name, email):")
df_forward = spark.sql("SELECT customer_id, name, email FROM lakehouse.tutorial.customers_newer")
df_forward.show()
print("✅ Forward compatible: Old query works on new schema!")
```

### Type Coercion Test

```python
# Create table with string type
spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_string (
    customer_id STRING,  -- Originally string
    name STRING
) USING ICEBERG
""")

# Insert data
spark.sql("""
INSERT INTO lakehouse.tutorial.customers_string VALUES
    ('1', 'Alice'),
    ('2', 'Bob')
""")

# Try to read as INT (will fail or coerce)
print("Reading customer_id as INT:")
try:
    spark.sql("SELECT CAST(customer_id AS INT) as customer_id, name FROM lakehouse.tutorial.customers_string").show()
    print("✅ Type coercion works!")
except Exception as e:
    print(f"Type coercion failed: {e}")

# Query directly
spark.sql("SELECT customer_id, name FROM lakehouse.tutorial.customers_string").show()
```

---

## 🎯 Part 7: Hands-on Exercises

Run these in Spark SQL (for example inside `spark.sql("...")`).

### Exercise 1: Add Columns and Query

```sql
-- 1. Add an 'address' column (no DEFAULT -- see Part 2)
-- ALTER TABLE lakehouse.tutorial.customers_evo ADD COLUMN address STRING;

-- 2. Insert new data with all columns. Spark needs a value for every column, in
--    table order; after Parts 2-5 the columns are customer_id, name, email_address,
--    address (check with DESCRIBE lakehouse.tutorial.customers_evo)
-- INSERT INTO lakehouse.tutorial.customers_evo VALUES (4, 'Dave', 'dave@example.com', '789 Pine Rd');

-- 3. Query with all columns
-- SELECT * FROM lakehouse.tutorial.customers_evo;

-- 4. Query with only old columns (backward compatibility)
-- SELECT customer_id, name, email_address FROM lakehouse.tutorial.customers_evo;
```

### Exercise 2: Remove Columns

```sql
-- 1. Add a throwaway column, then drop it in place (metadata only)
-- ALTER TABLE lakehouse.tutorial.customers_evo ADD COLUMN notes STRING;
-- ALTER TABLE lakehouse.tutorial.customers_evo DROP COLUMN notes;

-- 2. Or copy only the columns you want to keep into a clean table and swap the tables
-- CREATE TABLE lakehouse.tutorial.customers_clean AS SELECT customer_id, name, email_address, address FROM lakehouse.tutorial.customers_evo;
-- DROP TABLE lakehouse.tutorial.customers_evo PURGE;
-- ALTER TABLE lakehouse.tutorial.customers_clean RENAME TO tutorial.customers_evo;
```

### Exercise 3: Type Conversion

```sql
-- 1. Add a FLOAT column, then widen it to DOUBLE in place
-- ALTER TABLE lakehouse.tutorial.customers_num ADD COLUMN score FLOAT;
-- ALTER TABLE lakehouse.tutorial.customers_num ALTER COLUMN score TYPE DOUBLE;

-- 2. STRING -> INT is rejected in place; convert with CAST into a new table
-- CREATE TABLE lakehouse.tutorial.customers_int_age AS SELECT customer_id, name, CAST(age AS INT) AS age FROM lakehouse.tutorial.customers_num;

-- 3. Verify the conversion
-- DESCRIBE lakehouse.tutorial.customers_int_age;
```

### Exercise 4: Rename Columns

```sql
-- 1. Rename address to street_address
-- ALTER TABLE lakehouse.tutorial.customers_evo RENAME COLUMN address TO street_address;

-- 2. Query with new name
-- SELECT customer_id, name, street_address FROM lakehouse.tutorial.customers_evo;

-- 3. Try old name (will fail)
-- SELECT customer_id, name, address FROM lakehouse.tutorial.customers_evo;
```

---

## 📝 Summary

| Operation | Backward Compatible | Forward Compatible | Method |
|-----------|-------------------|-------------------|--------|
| **Add column** | ✅ Yes | ✅ Yes | `ALTER TABLE ADD COLUMN` |
| **Remove column** | ✅ Yes (old files still readable) | ⚠️ Queries using the column break | `ALTER TABLE DROP COLUMN` |
| **Update type** | ✅ For safe widening | ✅ For safe widening | `ALTER COLUMN ... TYPE`; otherwise new table with CAST |
| **Rename column** | ✅ Yes | ⚠️ Queries using the old name break | `ALTER TABLE RENAME COLUMN` |

The same changes in Trino (`iceberg.tutorial.<table>`):

| Operation | Trino SQL |
|-----------|-----------|
| Add column | `ALTER TABLE t ADD COLUMN phone varchar` |
| Drop column | `ALTER TABLE t DROP COLUMN phone` |
| Rename column | `ALTER TABLE t RENAME COLUMN email TO email_address` |
| Widen type | `ALTER TABLE t ALTER COLUMN customer_id SET DATA TYPE bigint` |

| Compatibility Type | Meaning | Use Case |
|-----------------|---------|----------|
| **Backward** | New reader can read old data | Adding columns safely |
| **Forward** | Old reader can read new data | Schema evolution without breaking clients |

---

## 🚀 Challenge Questions

1. **Can you evolve a schema that has a column removed and then re-added?**
2. **What happens if you add a column with the same name as a removed column?**
3. **Can you change a column type from STRING to INT without data loss?**
4. **How does schema evolution affect partitioning?**
5. **Can you maintain schema evolution history for auditing?**

---

## 📚 Additional Reading

- [Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [Type Promotions (spec)](https://iceberg.apache.org/spec/#schema-evolution)
- [Spark ALTER TABLE for Iceberg](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table)
- [Trino Iceberg connector: schema evolution](https://trino.io/docs/current/connector/iceberg.html#schema-evolution)
- [Connection Reference](../common/connection-reference.md)

---

**Schema evolution is one of Iceberg's most powerful features! Master it to build flexible, maintainable data pipelines that can adapt to changing requirements.**