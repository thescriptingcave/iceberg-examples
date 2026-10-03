# Iceberg Glossary

## A-Z Terms

### ACID
**A**tomicity, **C**onsistency, **I**solation, **D**urability
- **Atomicity**: All operations in a transaction succeed or fail together
- **Consistency**: Database remains in valid state before and after transaction
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Committed transactions persist even after system failures

### Bucketing
Dividing data into a fixed number of buckets based on a hash function. Used for high-cardinality columns where partitioning would create too many partitions.

**Example:**
```python
PARTITIONED BY (bucket(user_id, 10))  # 10 buckets
```

### Canonical Form
The standardized representation of Iceberg table metadata. Each table has one canonical metadata file that tracks the current state.

### Catalog
The metadata repository for Iceberg tables. Catalogs store:
- Table schemas
- Current snapshot pointers
- Access control policies
- Configuration properties

**Examples**: Polaris, AWS Glue, Hive Metastore, Nessie

In this tutorial the catalog is Apache Polaris, and the Polaris catalog is named `lakehouse`. Spark refers to it as `lakehouse`, Trino as `iceberg` (see the [Connection Reference](connection-reference.md)).

### Compaction
The process of merging small files into larger, more efficient files. Compaction improves:
- Query performance
- Storage efficiency
- Metadata overhead

### Data File
The actual data files in an Iceberg table, typically stored as Parquet, ORC, or Avro. Each data file contains a subset of table rows.

### Manifest
A file that lists data files and their metadata (like partition values, file sizes, row counts). Manifests are the bridge between snapshots and data files.

**Types**:
- **Manifest List**: Points to multiple manifest files
- **Manifest**: Points to individual data files

### Metadata File
JSON files that contain table metadata including:
- Schema definition
- Partition specifications
- Snapshot references
- Table properties

Metadata files are versioned (v1.metadata.json, v2.metadata.json, etc.)

### Metadata Log
A log of all metadata file changes. Each entry contains:
- Timestamp
- Metadata file location
- Snapshot ID (if applicable)

### Namespace
A logical grouping of tables, similar to a database or schema in traditional RDBMS. Namespaces can be nested (e.g., `sales.us.west`). Trino calls a namespace a *schema*. The labs use the namespace `tutorial`, so a table is `lakehouse.tutorial.customers` in Spark and `iceberg.tutorial.customers` in Trino.

### Open Table Format
Iceberg's specification for how data is organized and stored. Unlike Hive, Iceberg is an open standard that any engine can implement.

### Partition
Grouping a table's data files by the value of a column (or a transform of it, such as `day(ts)`). Partitions enable partition pruning - only reading relevant data. Iceberg records each file's partition value in its manifests, so pruning never depends on directory listing; the directories are only a naming convenience.

**Example:**
```
s3://warehouse/iceberg/tutorial/events/data/
├── ts_day=2024-01-01/
├── ts_day=2024-01-02/
└── ts_day=2024-01-03/
```

### Partition Spec
Defines how data is partitioned, including:
- Partition fields
- Transform functions (day, hour, bucket, etc.)
- Partition IDs

### Row Level Deletes
A feature that tracks deleted rows without rewriting data files (merge-on-read). Format v2 writes *delete files* (position or equality deletes), listed in delete manifests; format v3 replaces position delete files with *deletion vectors*.

### Schema
The column definitions for an Iceberg table, including:
- Column names
- Data types
- Nullability
- Comments
- Field IDs

Iceberg supports schema evolution, allowing you to add, remove, and rename columns over time.

### Snapshot
A point-in-time view of a table. Each snapshot contains:
- Snapshot ID
- Parent snapshot ID (or null)
- Timestamp
- Summary (operation type, files added/removed)
- Manifest list pointer

Snapshots form a chain, enabling time travel queries.

### Snapshot ID
A unique identifier for each snapshot. Can be used for time travel queries.

### Sort Order
Defines how data is ordered within files, enabling data pruning. Common sort orders include:
- **Z-order**: Multi-column clustering
- **Hilbert**: Multi-dimensional sorting

### Spec ID
Identifier for a partition specification. Each table can have multiple partition specs.

### Table UUID
A unique identifier for an Iceberg table, stored in its metadata. Commits can assert it, so a writer never accidentally commits to a different table that was dropped and re-created under the same name.

### Time Travel
Querying historical versions of a table. Iceberg supports:
- `VERSION AS OF snapshot_id`
- `TIMESTAMP AS OF '2024-01-01 12:00:00'`

### V3 Format
Iceberg format version 3, adding:
- Deletion vectors
- Row tracking
- VARIANT type

Tables in this tutorial use format version 2 (the default for Spark with Iceberg 1.9.1 and for Trino 477).

### Warehouse
The root location under which a catalog stores its tables. Typically organized as:
```
s3://warehouse/iceberg/          <- default-base-location of the `lakehouse` catalog
├── tutorial/                    <- namespace
│   └── customers/               <- table (data/ + metadata/)
└── ...
```

Careful: in an engine's REST catalog configuration (`spark.sql.catalog.<name>.warehouse`, Trino's `iceberg.rest-catalog.warehouse`), "warehouse" means something different for Polaris: it is the **name of the Polaris catalog** (`lakehouse`), not an S3 path.

## Advanced Terms

### Optimistic Locking
Polaris uses optimistic locking to handle concurrent writes. A commit states which snapshot it was based on; Polaris checks that this is still the current snapshot and rejects the commit (HTTP 409) if it isn't. The engine then re-reads the table and retries.

### Credential Vending
A catalog feature where the catalog hands an engine short-lived, down-scoped storage credentials for just the table it is accessing. Polaris mints them through AWS STS. Garage, the object store in this tutorial, has no STS, so vending is switched off (`stsUnavailable: true`) and engines use a static key instead.

### Remote Signing
A security feature where the catalog pre-signs each file access instead of handing out storage credentials.

### Scan Planning
The process of determining which files to read for a query. Server-side scan planning offloads this work to the catalog.

### Schema Projection
Reading only the columns needed for a query, ignoring the rest. Enabled by column pruning.

### Partition Pruning
Reading only the partitions needed for a query, ignoring the rest. Enabled by partition filtering.

## Common Acronyms

| Acronym | Meaning |
|---------|---------|
| **ACID** | Atomicity, Consistency, Isolation, Durability |
| **DAG** | Directed Acyclic Graph (Spark's execution model) |
| **ETL** | Extract, Transform, Load |
| **FaaS** | Functions as a Service |
| **FIPS** | Federal Information Processing Standards |
| **GA** | General Availability |
| **IAM** | Identity and Access Management |
| **KMS** | Key Management Service |
| **ML** | Machine Learning |
| **MVP** | Minimum Viable Product |
| **OSS** | Open Source Software |
| **QoS** | Quality of Service |
| **REST** | Representational State Transfer |
| **RBAC** | Role-Based Access Control |
| **S3** | Simple Storage Service |
| **SLA** | Service Level Agreement |
| **SQL** | Structured Query Language |
| **UTC** | Coordinated Universal Time |
| **VPC** | Virtual Private Cloud |
| **ZK** | ZooKeeper |

## Iceberg Version History

| Version | Release | Key Features |
|---------|---------|--------------|
| v1 | 2019 | Initial specification, basic features |
| v2 | 2021 | Row-level deletes (delete files), upserts |
| v3 | 2025 | Deletion vectors, row tracking, VARIANT type |
| v4 | in development | Not yet finalized |

## Related Technologies

| Technology | Relationship to Iceberg |
|------------|------------------------|
| **Apache Spark** | Compute engine that reads/writes Iceberg tables |
| **Trino** | Distributed SQL query engine with an Iceberg connector (not an Apache project) |
| **Apache Parquet** | Columnar file format used by Iceberg |
| **Apache ORC** | Alternative columnar file format |
| **Delta Lake** | competing table format (not compatible) |
| **Hudi** | competing table format (some compatibility) |
| **AWS Glue** | Catalog option for Iceberg |
| **Apache Polaris** | Open-source Iceberg REST catalog (used in this tutorial) |
| **Garage** | Lightweight S3-compatible object store (used in this tutorial) |
| **Nessie** | Catalog with Git-style branching |