# Iceberg Architecture Reference

## The Three-Layer Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    ICEBERG ARCHITECTURE                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    LAYER 3: COMPUTE                            │  │
│  │          (this stack: Spark 3.5 in JupyterLab, Trino 477)      │  │
│  │                                                                │  │
│  │   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐       │  │
│  │   │    Spark     │   │    Trino     │   │  Flink, ...  │       │  │
│  │   │  + Iceberg   │   │  Iceberg     │   │ (not in this │       │  │
│  │   │    1.9.1     │   │  connector   │   │    stack)    │       │  │
│  │   └──────────────┘   └──────────────┘   └──────────────┘       │  │
│  │                                                                │  │
│  │  What it does:                                                 │  │
│  │  • Asks the catalog where the table's current metadata is      │  │
│  │  • Reads manifests + data files straight from storage          │  │
│  │  • Processes and transforms data                               │  │
│  │  • Writes new data files straight to storage                   │  │
│  │  • Asks the catalog to commit the new snapshot                 │  │
│  └───────────┬──────────────────────────────────────┬─────────────┘  │
│              │ Iceberg REST (HTTP + OAuth2)         │ S3 API         │
│              ▼                                      │                │
│  ┌──────────────────────────────────────────┐       │                │
│  │           LAYER 2: CATALOG               │       │                │
│  │   (this stack: Apache Polaris 1.8;       │       │                │
│  │    elsewhere: Glue, Hive, Nessie, ...)   │       │                │
│  │                                          │       │                │
│  │  What it holds:                          │       │                │
│  │  • Namespaces and the list of tables     │       │                │
│  │  • Pointer to each table's current       │       │                │
│  │    metadata file                         │       │                │
│  │  • Principals, roles and grants          │       │                │
│  │                                          │       │                │
│  │  Its own state lives in PostgreSQL.      │       │                │
│  └──────────────────┬───────────────────────┘       │                │
│                     │ S3 API (writes metadata.json) │                │
│                     ▼                               ▼                │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    LAYER 1: STORAGE                            │  │
│  │      (this stack: Garage; elsewhere: S3, GCS, ADLS, ...)       │  │
│  │                                                                │  │
│  │  s3://warehouse/iceberg/<namespace>/<table>/                   │  │
│  │  ├─ data/                                                      │  │
│  │  │  ├─ 00000-...-00001.parquet                                 │  │
│  │  │  └─ 00001-...-00001.parquet                                 │  │
│  │  └─ metadata/                                                  │  │
│  │     ├─ 00000-<uuid>.metadata.json                              │  │
│  │     ├─ 00001-<uuid>.metadata.json (current)                    │  │
│  │     ├─ snap-<snapshot-id>-...avro  (manifest lists)            │  │
│  │     └─ <uuid>-m0.avro              (manifests)                 │  │
│  │                                                                │  │
│  │  What it holds:                                                │  │
│  │  • Parquet data files (actual table data)                      │  │
│  │  • Manifest files (lists of data files + their stats)          │  │
│  │  • Manifest lists (one per snapshot, points to manifests)      │  │
│  │  • Metadata files (schema, partition spec, snapshot list)      │  │
│  │                                                                │  │
│  │  Key point: This is just files. No idea of "tables" here!      │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

Key point: the engines **do** talk to storage directly -- every data file is
read and written by Spark or Trino itself. What they get from the catalog is
*which* metadata file is current, and a safe way to swap it for a new one.

## What Runs in This Tutorial

| Container | Image | Role |
|---|---|---|
| `iceberg-objectstore` | `dxflrs/garage:v2.1.0` | S3-compatible object store (Garage), single node, S3 API on port 3900 |
| `iceberg-objectstore-bootstrap` | built from `config/objectstore/` | One-shot: makes Garage usable, then exits |
| `iceberg-postgres` | `postgres:16-alpine` | Polaris' own database (catalogs, namespaces, table pointers, principals, grants) |
| `iceberg-polaris` | `apache/polaris:1.8.0` | Iceberg REST catalog, port 8181 |
| `iceberg-polaris-bootstrap` | `apache/polaris-admin-tool:1.8.0` | One-shot: bootstraps the realm and creates the `lakehouse` catalog, then exits |
| `iceberg-trino` | `trinodb/trino:477` | Interactive SQL engine, port 8080 |
| `iceberg-jupyter` | built from `config/jupyter/` (`jupyter/pyspark-notebook:spark-3.5.0` + Iceberg 1.9.1) | JupyterLab with Spark, ports 8888 and 4040 |

Start-up order, enforced by `depends_on` in `docker-compose.yml`:

```
objectstore ──▶ objectstore-bootstrap ──┐   (writes /creds/garage-credentials.env)
postgres (healthy) ─────────────────────┼──▶ polaris ──▶ polaris-bootstrap ──▶ trino
                                        │                                  └─▶ jupyter
                                        └──────────────────────────────────────▶ (all read /creds)
```

The two bootstrap containers are expected to show `Exited (0)` in
`docker compose ps -a`. Both are idempotent and run again on every
`docker compose up`.

For every name, port and credential, see the
[Connection Reference](connection-reference.md).

## Component Details

### Layer 1: Storage (Garage)

[Garage](https://garagehq.deuxfleurs.fr/) is a lightweight S3-compatible
object store. Here it runs as a single node with replication factor 1. It has
no web console; inspect it with its CLI:

```bash
docker exec iceberg-objectstore /garage bucket info warehouse
```

A fresh Garage node serves nothing until it has a cluster layout, a bucket and
an access key. `objectstore-bootstrap` (`config/objectstore/bootstrap.sh`) does
that on every start:

1. waits for Garage, then assigns and applies a cluster layout (first boot only);
2. creates an access key named `iceberg` (first boot only -- Garage *generates*
   the key ID and secret, so they are not known in advance);
3. creates the `warehouse` bucket and grants the key read/write/owner on it;
4. writes the key to `/creds/garage-credentials.env` on the shared
   `garage-creds` volume.

Polaris, Trino and Jupyter all mount that volume read-only and load the key
from it at start-up. That is why no S3 key appears anywhere in the repo.

**Table layout:** Polaris places tables under the catalog's
`default-base-location`, `s3://warehouse/iceberg`, giving
`s3://warehouse/iceberg/<namespace>/<table>/`. Tables created from Trino get a
random suffix on the directory (`<table>-<uuid>/`) so that re-creating a table
with the same name never reuses old files.

**Key Files:**
- **Data Files**: Parquet files containing actual table data (`data/`)
- **Manifest Files**: List data files with their partition values and column statistics (`metadata/*-m0.avro`)
- **Manifest Lists**: One per snapshot, listing that snapshot's manifests (`metadata/snap-*.avro`)
- **Metadata Files**: Table schema, partition specs, properties and the list of snapshots (`metadata/*.metadata.json`)

### Layer 2: Catalog (Polaris + PostgreSQL)

Apache Polaris 1.8 implements the Iceberg REST catalog protocol. It keeps its
own state -- realms, catalogs, namespaces, the pointer to each table's current
metadata file, principals, roles and grants -- in PostgreSQL
(`POLARIS_PERSISTENCE_TYPE=relational-jdbc`, schema `polaris_schema`, created
by `config/polaris/postgres-schema.sql` on the database's first start). Without
that, Polaris would run in memory and forget everything on restart.

Polaris also reads and writes the table metadata files in Garage itself, so its
entrypoint (`config/polaris/entrypoint.sh`) loads the Garage key from `/creds`
before starting the server.

`polaris-bootstrap` (`config/polaris/bootstrap.sh`) turns an empty Polaris into
a usable one:

1. **bootstraps the realm `POLARIS`** with the root principal `root`/`root`.
   The server does not do this itself; an unbootstrapped realm has no
   credentials and every token request fails;
2. **creates the catalog `lakehouse`** through the management API, with
   storage type S3, endpoint `http://objectstore:3900`, `pathStyleAccess: true`,
   `allowedLocations: ["s3://warehouse"]` and **`stsUnavailable: true`**
   (if it already exists, the same settings are re-applied);
3. sets the catalog property **`polaris.config.drop-with-purge.enabled=true`**,
   because Trino's `DROP TABLE` always asks Polaris to purge the files;
4. **grants `CATALOG_MANAGE_CONTENT`** to the `catalog_admin` catalog role, so
   the root principal may read/write table data and drop-with-purge, not just
   manage the catalog.

**Endpoints** (base `http://localhost:8181`, every call needs an OAuth2 bearer
token -- see the [Connection Reference](connection-reference.md#polaris-rest-api)):

- `POST /api/catalog/v1/oauth/tokens` - Get a token (client credentials)
- `GET  /api/catalog/v1/config?warehouse=lakehouse` - Catalog config an engine fetches first
- `GET  /api/catalog/v1/lakehouse/namespaces` - List namespaces
- `GET  /api/catalog/v1/lakehouse/namespaces/{ns}/tables` - List tables
- `POST /api/catalog/v1/lakehouse/namespaces/{ns}/tables` - Create table
- `GET  /api/catalog/v1/lakehouse/namespaces/{ns}/tables/{table}` - Load table (metadata, incl. all snapshots)
- `POST /api/catalog/v1/lakehouse/namespaces/{ns}/tables/{table}` - Commit changes
- `GET  /api/management/v1/catalogs` - Management API: catalogs, principals, roles, grants

There is no separate "list snapshots" endpoint: snapshots are part of the
table metadata returned by *load table*.

**Features:**
- Metadata resolution
- Access control (principals, roles, grants)
- Credential vending -- **switched off in this stack**, see below
- Snapshot management
- Commit conflict detection

**Why credential vending is off.** Normally Polaris can hand each engine
short-lived, down-scoped S3 credentials for just the table it is accessing,
minted through AWS STS. Garage has no STS endpoint, so the catalog is created
with `stsUnavailable: true`, Trino sets
`iceberg.rest-catalog.vended-credentials-enabled=false`, and every engine uses
the static Garage key from `/creds/garage-credentials.env` instead. Polaris
still decides *who may access which table*; it just cannot enforce it at the
storage level.

### Layer 3: Compute (Spark/Trino)

**Spark (JupyterLab):** `jupyter/pyspark-notebook:spark-3.5.0` with
`iceberg-spark-runtime-3.5_2.12-1.9.1.jar` and `iceberg-aws-bundle-1.9.1.jar`
added to `$SPARK_HOME/jars`. The Spark catalog is named `lakehouse` and is the
default catalog, so tables are `lakehouse.tutorial.<table>`. The full
`SparkSession` configuration is in the
[Connection Reference](connection-reference.md#spark-in-a-jupyter-notebook).

**Trino 477:** one catalog, `iceberg`, rendered at start-up by
`config/trino/entrypoint.sh` from
`config/trino/catalog/iceberg.properties.template` (the template has
placeholders because the Garage key is only known at runtime). Tables are
`iceberg.tutorial.<table>`.

## Transaction Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                    TRANSACTION FLOW                                  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. CLIENT (Spark/Trino)                                             │
│     └─ SQL: INSERT INTO tutorial.customers VALUES (...)              │
│                                                                      │
│  2. COMPUTE ENGINE                                                   │
│     ├─ Parse SQL                                                     │
│     ├─ Validate schema                                               │
│     └─ Create execution plan                                         │
│                                                                      │
│  3. CATALOG REQUEST (load table)                                     │
│     └─ GET /api/catalog/v1/lakehouse/namespaces/tutorial/            │
│            tables/customers                                          │
│                                                                      │
│  4. CATALOG RESPONSE                                                 │
│     └─ Returns: current metadata (current-snapshot-id: 10)           │
│                                                                      │
│  5. COMPUTE WRITES FILES TO STORAGE (directly, over S3)              │
│     ├─ Process new data                                              │
│     ├─ Write data files (data/00000-...parquet)                      │
│     ├─ Write manifest (metadata/<uuid>-m0.avro)                      │
│     └─ Write manifest list for snapshot 11 (metadata/snap-11-...)    │
│        Nobody can see these files yet -- no snapshot points to them  │
│                                                                      │
│  6. COMMIT REQUEST                                                   │
│     └─ POST /api/catalog/v1/lakehouse/namespaces/tutorial/           │
│             tables/customers                                         │
│        {                                                             │
│          "requirements": [{"type": "assert-ref-snapshot-id",         │
│                            "ref": "main", "snapshot-id": 10}],       │
│          "updates": [{"action": "add-snapshot", ...},                │
│                      {"action": "set-snapshot-ref",                  │
│                       "ref-name": "main", "snapshot-id": 11, ...}]   │
│        }                                                             │
│                                                                      │
│  7. CATALOG VALIDATION                                               │
│     ├─ Check: Is the current snapshot still 10?                      │
│     ├─ YES → write next metadata.json, swap the pointer atomically   │
│     └─ NO → Reject (conflict detected)                               │
│                                                                      │
│  8. CATALOG RESPONSE                                                 │
│     ├─ Success → 200 OK  → the new rows are now visible              │
│     └─ Conflict → 409 Conflict → engine re-reads and retries         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

(The `main` in the commit request is Iceberg's default *branch* of a table --
unrelated to catalog or namespace names.)

## Configuration Reference

Don't copy settings from here -- the exact, tested configuration lives in one
place: the [Connection Reference](connection-reference.md). In summary:

### Spark Configuration
- catalog `lakehouse` of type `rest`, `uri=http://polaris:8181/api/catalog`,
  `warehouse=lakehouse` (the Polaris catalog **name**, not an S3 path)
- OAuth2 `credential=root:root`, `scope=PRINCIPAL_ROLE:ALL`
- `S3FileIO` with `s3.endpoint=http://objectstore:3900`,
  `s3.path-style-access=true` and the Garage key read from
  `/creds/garage-credentials.env`
- `spark.sql.defaultCatalog=lakehouse`

### Trino Configuration
```properties
# config/trino/catalog/iceberg.properties.template (rendered at start-up)
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${POLARIS_URI}                 # http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=${POLARIS_CATALOG}       # lakehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.server-uri=${POLARIS_TOKEN_URI}
iceberg.rest-catalog.oauth2.credential=${POLARIS_CREDENTIAL}   # root:root
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.vended-credentials-enabled=false
fs.native-s3.enabled=true
s3.endpoint=${S3_ENDPOINT}                              # http://objectstore:3900
s3.region=${S3_REGION}
s3.path-style-access=true
s3.aws-access-key=${AWS_ACCESS_KEY_ID}                  # from /creds
s3.aws-secret-key=${AWS_SECRET_ACCESS_KEY}
```

## Performance Optimization

### File Size Recommendations
- **Optimal**: 128MB - 1GB per file (Iceberg's default target is 512MB)
- **Too small**: <10MB (causes overhead)
- **Too large**: >2GB (memory issues)

### Partition Recommendations
- **Optimal**: 1M - 100M rows per partition
- **Too small**: <10K rows (too many partitions)
- **Too large**: >1B rows (large scans)

### Snapshot Expiration
- **Recommended**: 7 days
- **Minimum**: 1 day
- **Maximum**: 30 days

(Trino refuses `expire_snapshots` / `remove_orphan_files` retention below 7 days
unless you lower the limit -- see the
[Performance lab](../bonus-performance/README.md).)

## Monitoring Metrics

| Metric | Good | Bad |
|--------|------|-----|
| File count per partition | <10 | >100 |
| Average file size | 100MB - 1GB | <1MB or >2GB |
| Partition count | <1000 | >10000 |
| Snapshot age | <7 days | >30 days |
| Small file count | <10 | >100 |

## Security Model

```
┌──────────────────────────────────────────────────────────────────────┐
│                    SECURITY MODEL                                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                  Polaris Access Control                         │ │
│  │                                                                 │ │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     │ │
│  │  │  Principal   │────▶│  Principal   │────▶│  Catalog     │     │ │
│  │  │  (User/      │     │  Roles       │     │  Roles       │     │ │
│  │  │   Service)   │     │              │     │              │     │ │
│  │  └──────────────┘     └──────────────┘     └──────────────┘     │ │
│  │   root                 service_admin        catalog_admin       │ │
│  │                                             (on lakehouse)      │ │
│  │                                                                 │ │
│  │  Privileges are granted to catalog roles, e.g.:                 │ │
│  │  • Table-level: TABLE_READ_DATA, TABLE_WRITE_DATA, TABLE_CREATE │ │
│  │  • Namespace-level: NAMESPACE_CREATE, NAMESPACE_DROP            │ │
│  │  • Catalog-level: CATALOG_MANAGE_CONTENT,                       │ │
│  │    CATALOG_MANAGE_METADATA, CATALOG_MANAGE_ACCESS               │ │
│  │                                                                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

In this stack everything runs as `root`. Requesting a token with
`scope=PRINCIPAL_ROLE:ALL` activates its principal role `service_admin`, which
holds the catalog role `catalog_admin` on `lakehouse`. Out of the box that role
has `CATALOG_MANAGE_ACCESS` and `CATALOG_MANAGE_METADATA`;
`polaris-bootstrap` adds `CATALOG_MANAGE_CONTENT`. Check with:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants
```

## Troubleshooting

### Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Commit conflict | 409 error / `CommitFailedException` | Retry; engines re-read the latest snapshot |
| Permission denied | 403 error | Check the grants above; see [Troubleshooting](troubleshooting.md) |
| Snapshot expired | Missing data | Increase retention period |
| Network error | Connection refused | Check service status (`docker compose ps -a`) |

### Logs to Check

1. **Spark**: the notebook output, and the Spark UI at http://localhost:4040 while a session is running
2. **Trino**: `docker logs iceberg-trino`
3. **Polaris**: `docker logs iceberg-polaris`, `docker logs iceberg-polaris-bootstrap`
4. **Garage**: `docker logs iceberg-objectstore`, `docker logs iceberg-objectstore-bootstrap`

See the [Troubleshooting Guide](troubleshooting.md) for the failure modes this
stack actually runs into.

---

**This architecture reference provides the foundational understanding needed to work with Iceberg effectively!**
