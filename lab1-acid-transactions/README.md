# ACID Transactions Lab Summary

## 📚 Overview

This lab covers **ACID Transactions** in Apache Iceberg through three different tools: Spark, Trino, and Polaris.

## 🔑 Key Concepts

### What is ACID?

| Property | Definition | How Iceberg Implements It |
|----------|------------|--------------------------|
| **Atomicity** | All operations in a transaction succeed or fail together | Metadata commits are atomic |
| **Consistency** | Database remains in valid state | Schema validation before commit |
| **Isolation** | Concurrent transactions don't interfere | Snapshot isolation + optimistic locking |
| **Durability** | Committed transactions persist | Data and metadata files written to object storage (Garage) before the catalog commit; Polaris stores the table pointer in PostgreSQL |

### The Three-Layer Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    ACID Transaction Flow                      │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Client (Spark/Trino)                                     │
│     └── Sends transaction: INSERT, UPDATE, DELETE           │
│                                                               │
│  2. Catalog (Polaris)                                        │
│     ├── Reads current snapshot                                │
│     ├── Validates permissions                                 │
│     ├── Checks: "Is current snapshot unchanged?"             │
│     └── Commits if OK, rejects if conflict                  │
│                                                               │
│  3. Storage (Garage, S3 API)                                 │
│     ├── Stores data files                                     │
│     ├── Stores metadata files                                 │
│     └── Persists snapshots                                    │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## 🛠️ Tool-Specific Implementation

### Spark Implementation

**Use Spark for:**
- Complex ETL operations
- Batch processing
- Large-scale data transformations

**Key Commands:**
```python
# Create table
spark.sql("CREATE TABLE lakehouse.tutorial.customers (...) USING ICEBERG")

# Insert data
spark.sql("INSERT OVERWRITE lakehouse.tutorial.customers VALUES (...)")

# Update data
spark.sql("UPDATE lakehouse.tutorial.customers SET ... WHERE ...")

# Delete data
spark.sql("DELETE FROM lakehouse.tutorial.customers WHERE ...")

# Upsert
spark.sql("MERGE INTO lakehouse.tutorial.customers t USING updates s ON ... WHEN MATCHED ... WHEN NOT MATCHED ...")

# Commit history
spark.sql("SELECT * FROM lakehouse.tutorial.customers.snapshots")
```

See [`common/connection-reference.md`](../common/connection-reference.md) for the `SparkSession` setup.

### Trino Implementation

**Use Trino for:**
- Interactive SQL queries
- Ad-hoc analysis
- Dashboard queries

**Key Commands:**
```sql
-- Create table
CREATE TABLE iceberg.tutorial.customers (...) WITH (partitioning = ARRAY['column']);

-- Insert data. Trino has no INSERT OVERWRITE (Spark does), so clear the
-- table first if you want the step to be safe to re-run.
DELETE FROM iceberg.tutorial.customers;
INSERT INTO iceberg.tutorial.customers VALUES (...);

-- Update data
UPDATE iceberg.tutorial.customers SET ... WHERE ...;

-- Delete data
DELETE FROM iceberg.tutorial.customers WHERE ...;

-- Commit history
SELECT * FROM iceberg.tutorial."customers$snapshots";
```

Each statement is its own atomic transaction (one Iceberg snapshot). The Trino
Iceberg connector does **not** support multi-statement write transactions:
`START TRANSACTION; INSERT ...; COMMIT;` fails with
`Catalog only supports writes using autocommit: iceberg`.

### Polaris Implementation

**Use Polaris for:**
- Catalog operations
- Access control
- Transaction monitoring

**Key Commands:**
```bash
# Get a bearer token (OAuth2 client credentials, root/root)
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=root \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

# List namespaces
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces

# Get table metadata (includes the snapshot list and current-snapshot-id)
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers

# Commit changes: "requirements" are the optimistic-concurrency checks,
# "updates" are applied only if every requirement holds (otherwise 409)
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers \
  -d '{"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": <current-snapshot-id>}],
       "updates": [{"action": "set-properties", "updates": {"owner": "lab1"}}]}'
```

There is no separate `/snapshots` endpoint: snapshots are part of the table
metadata returned by the GET above. See
[`common/connection-reference.md`](../common/connection-reference.md) for more.

## 🔍 Common Pitfalls

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Commit Conflict** | 409 `CommitFailedException` ("branch main has changed") from Polaris, or `ValidationException: Found conflicting files` in Spark | Re-read the table and retry; Iceberg already retries non-conflicting commits automatically |
| **Permission Denied** | 403 error | Check Polaris access control; make sure the token was requested with `scope=PRINCIPAL_ROLE:ALL` |
| **Snapshot Expired** | Missing snapshots | Increase retention period |
| **Not Authorized** | 401 error | Bearer token missing or expired (tokens last one hour); request a new one |
| **Network Error** | Connection refused | Wait for services to start |

## 📊 Best Practices

### For Spark
1. Use transactions for batch operations
2. Handle commit conflicts with retry logic
3. Monitor Spark UI for performance issues

### For Trino
1. Treat each statement as one transaction -- multi-statement `START TRANSACTION`/`COMMIT` is not supported for Iceberg writes
2. Put multi-step changes into a single `MERGE` when they must be atomic
3. Monitor concurrent query performance

### For Polaris
1. Configure access control for security
2. Monitor snapshot history for issues
3. Set up alerting for commit conflicts

## 🚀 Next Steps

- **Lab 2: Time Travel** - Query historical versions of tables
- **Lab 3: Schema Evolution** - Modify table schemas over time
- **Bonus Lab: Performance** - Optimize your Iceberg tables

---

**Remember: ACID is not just a acronym - it's the foundation of reliable data engineering!**