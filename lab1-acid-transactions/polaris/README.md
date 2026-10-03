# Lab 1: ACID Transactions (Polaris Implementation)

## 🎯 Learning Objectives

By the end of this lab, you will:

1. **Understand Polaris as the ACID enforcement layer** for Iceberg
2. **Work with Polaris REST API** for catalog operations
3. **Manage commits and snapshots** via Polaris
4. **Implement access control** for transaction operations
5. **Monitor transaction health** through Polaris
6. **Compare Polaris with Spark/Trino** for ACID handling

---

## 📚 Part 1: Understanding Polaris as the ACID Enforcer

### What is Polaris?

Apache Polaris is the open-source Iceberg REST Catalog implementation. It's the **ACID enforcement layer** for Iceberg.

```
┌──────────────────────────────────────────────────────────────────┐
│           Polaris Architecture                                  │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Polaris Server                        │  │
│  │                                                            │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │ │   REST API   │  │   Authz      │  │   Catalog    │   │  │
│  │ │   (HTTP)     │  │   (Access)   │  │   Service    │   │  │
│  │ └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  │         │                   │             │              │  │
│  └─────────┼───────────────────┼─────────────┼──────────────┘  │
│            │                   │             │                 │
│            ▼                   ▼             ▼                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Persistence Layer                      │  │
│  │                                                            │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │ │   PostgreSQL │  │   Metadata   │  │   Config     │   │  │
│  │ │   Database   │  │   Pointers   │  │   Policies   │   │  │
│  │ └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Polaris receives REST API calls and:                          │
│  1. Validates permissions (Authorization)                      │
│  2. Manages snapshots (Catalog)                                │
│  3. Persists metadata (Database)                               │
│  4. Enforces ACID by atomically swapping the metadata pointer  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Polaris ACID Enforcement Mechanisms

| ACID Property | Polaris Implementation |
|--------------|----------------------|
| **Atomicity** | All-or-nothing metadata commits |
| **Consistency** | Schema validation before commit |
| **Isolation** | Current snapshot ID checking (optimistic locking) |
| **Durability** | PostgreSQL persistence before response |

### How Polaris Enforces ACID

```
┌──────────────────────────────────────────────────────────────────┐
│           Polaris ACID Enforcement Flow                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Spark/Trino sends: "Current snapshot: snap-10"            │
│                                                                  │
│  2. Polaris validates: "Is current snapshot still snap-10?"   │
│                                                                  │
│  3a. YES → Polaris updates to snap-11                           │
│      │                                                          │
│      ├── Write new metadata.json to the object store          │
│      ├── Swap the table's metadata pointer in PostgreSQL       │
│      └── Return: "Commit successful"                           │
│                                                                  │
│  3b. NO → Polaris rejects with conflict                        │
│      │                                                          │
│      └── Return: "409 Conflict: current snapshot changed"     │
│                                                                  │
│  This is Polaris's optimistic locking mechanism!               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Polaris REST API Endpoints

Polaris serves two APIs on port 8181 (see
[`common/connection-reference.md`](../../common/connection-reference.md)):

- the **Iceberg REST catalog API** under `/api/catalog/v1/...` -- note that
  table paths include the catalog name, `lakehouse`
- the **management API** (catalogs, principals, roles, grants) under
  `/api/management/v1/...`

Every call needs an OAuth2 bearer token. Get one with the `root` client
credentials (the `scope` activates root's roles -- without it every call is 403):

```bash
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=root \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
```

Tokens expire after one hour; if you get a `401`, run this again.

```bash
CAT=http://localhost:8181/api/catalog/v1/lakehouse

# List namespaces
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces

# List tables in a namespace
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables

# Get (load) table metadata -- includes the snapshot list and history
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers

# Update table (commit) -- what Spark and Trino call on every write
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables/customers \
  -d '{"requirements": [...], "updates": [...]}'

# Commit changes to several tables atomically
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/transactions/commit \
  -d '{"table-changes": [...]}'
```

There is no separate "list snapshots" or "history" endpoint: snapshots and
history are part of the table metadata returned by the load-table call.

---

## 🛠️ Part 2: Working with Polaris REST API

### Step 1: Verify Polaris is Running

```bash
# Without a token Polaris answers 401 -- that alone proves it is up
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/api/catalog/v1/config

# With a token, ask for the configuration of the lakehouse catalog
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8181/api/catalog/v1/config?warehouse=lakehouse"
```

Expected response (shortened):

```json
{
  "defaults": {
    "polaris.config.drop-with-purge.enabled": "true",
    "default-base-location": "s3://warehouse/iceberg"
  },
  "overrides": {
    "namespace-separator": "%1F",
    "prefix": "lakehouse"
  },
  "endpoints": [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/transactions/commit",
    "..."
  ]
}
```

This is the first call every engine makes. The `prefix` override is why every
other path contains `/lakehouse/`. Polaris has no version endpoint; the version
is the image tag (`docker inspect iceberg-polaris --format '{{.Config.Image}}'`).

### Step 2: List Namespaces

```bash
# List all namespaces
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces
```

Expected response (each namespace is an array of name parts):

```json
{
  "namespaces": [
    ["tutorial"]
  ],
  "next-page-token": null
}
```

### Step 3: Create a Namespace

```bash
# Create a namespace
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces \
  -d '{
    "namespace": ["lab1_rest"],
    "properties": {
      "comment": "Namespace created through the REST API"
    }
  }'
```

Expected response (`200`). Polaris adds the storage location itself:

```json
{"namespace":["lab1_rest"],"properties":{"comment":"Namespace created through the REST API","location":"s3://warehouse/iceberg/lab1_rest/"}}
```

Run the same command again and you get `409`
(`"type":"AlreadyExistsException"`). Remove the namespace when you are done
(`204`, it must be empty):

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE \
  -H "Authorization: Bearer $TOKEN" $CAT/namespaces/lab1_rest
```

### Step 4: List Tables

```bash
# List tables in the tutorial namespace
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables
```

```json
{"identifiers":[{"namespace":["tutorial"],"name":"customers"}, ...],"next-page-token":null}
```

### Step 5: Get Table Metadata

```bash
# Get table metadata
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers
```

The response has three top-level keys:

- `metadata-location` -- the current `*.metadata.json` file in the object store
  (this pointer is what Polaris stores and swaps on each commit)
- `metadata` -- the full table metadata, including:
  - `schemas`, `current-schema-id`
  - `current-snapshot-id` and `snapshots` (history of snapshots)
  - `snapshot-log` and `metadata-log`
  - `partition-specs`
  - `properties`
  - `table-uuid`
- `config` -- the S3 settings clients should use (endpoint, path-style access)

---

## 🔍 Part 3: Managing Commits and Snapshots

### Understanding Snapshots in Polaris

Each Iceberg table has a history of snapshots. The files live in the object
store; Polaris only stores the location of the current metadata file:

```
s3://warehouse/iceberg/tutorial/customers-<uuid>/
├── data/
│   └── customer_id=1/....parquet          ← data (and delete) files
└── metadata/
    ├── 00000-<uuid>.metadata.json         ← metadata version 0
    ├── 00001-<uuid>.metadata.json         ← metadata version 1
    ├── 00002-<uuid>.metadata.json         ← current (Polaris points here)
    ├── snap-<snapshot-id>-1-<uuid>.avro   ← manifest list, one per snapshot
    └── <uuid>-m0.avro                     ← manifests
```

### Listing Snapshots

```bash
# Snapshots are part of the load-table response; pick them out with jq
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers \
  | jq '.metadata.snapshots'
```

Response (shortened):

```json
[
  {
    "sequence-number": 1,
    "snapshot-id": 3051729675574597004,
    "timestamp-ms": 1704067200000,
    "summary": {
      "operation": "append",
      "added-data-files": "3",
      "added-records": "3",
      "total-records": "3"
    },
    "manifest-list": "s3://warehouse/iceberg/tutorial/customers-<uuid>/metadata/snap-3051729675574597004-1-<uuid>.avro",
    "schema-id": 0
  }
]
```

### Creating a New Snapshot (Commit)

```bash
# To commit a new snapshot, Spark/Trino send something like:
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables/customers \
  -d '{
    "requirements": [
      {"type": "assert-table-uuid", "uuid": "<table-uuid>"},
      {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": <current-snapshot-id>}
    ],
    "updates": [
      {"action": "add-snapshot", "snapshot": {...}},
      {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": <new-snapshot-id>}
    ]
  }'
```

**What's happening here?**

| Requirement | Purpose |
|-------------|---------|
| `assert-table-uuid` | Ensure we're updating the right table (not a dropped-and-recreated one) |
| `assert-ref-snapshot-id` | Ensure no one else moved the `main` branch since we read it (optimistic locking) |

(`main` here is Iceberg's default **branch**, not a namespace.)

The engine has already written the data files and the new snapshot's manifests
before this call. Polaris only checks the requirements, writes the new metadata
file and swaps its pointer -- all or nothing.

You can make a real, harmless commit yourself by changing a table property:

```bash
SNAP=$(curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers \
  | jq '.metadata["current-snapshot-id"]')

curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables/customers \
  -d "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":$SNAP}],
       \"updates\":[{\"action\":\"set-properties\",\"updates\":{\"lab1.owner\":\"polaris-lab\"}}]}"
# 200 -- and metadata-location now points at a new metadata.json
```

### Commit Conflict Detection

```bash
# Simulate a conflict scenario: a writer whose view of the table is out of date.
# We fake that by asserting a snapshot id that is not the current one.
OLD=1

# Try to commit as a writer that still believes OLD is current
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables/customers \
  -d "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":$OLD}],
       \"updates\":[{\"action\":\"set-properties\",\"updates\":{\"lab1.owner\":\"stale-writer\"}}]}"
```

Expected response: HTTP `409 Conflict`

```json
{"error":{"message":"Requirement failed: branch main has changed: expected id 1 != 8836187362871233711","type":"CommitFailedException","code":409}}
```

Nothing was changed; the
engine's job is to re-read the table, re-apply its change and try again.

### Multi-Table Atomic Commits

`POST $CAT/transactions/commit` applies changes to **several tables** in one
all-or-nothing commit: if any table's requirements fail, none of the tables
change.

```bash
CUST=$(curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers | jq '.metadata["current-snapshot-id"]')
ORD=$(curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/orders | jq '.metadata["current-snapshot-id"]')

curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $CAT/transactions/commit -d "{\"table-changes\":[
    {\"identifier\":{\"namespace\":[\"tutorial\"],\"name\":\"customers\"},
     \"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":$CUST}],
     \"updates\":[{\"action\":\"set-properties\",\"updates\":{\"lab1.batch\":\"42\"}}]},
    {\"identifier\":{\"namespace\":[\"tutorial\"],\"name\":\"orders\"},
     \"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":$ORD}],
     \"updates\":[{\"action\":\"set-properties\",\"updates\":{\"lab1.batch\":\"42\"}}]}]}"
# 204: both tables now have lab1.batch=42
```

Replace `$ORD` with a wrong id (e.g. `1`) and the call returns `409`, and
**neither** table gets the property.

---

## 📊 Part 4: Monitoring Transaction Health

### Monitoring Snapshots

```bash
# One line per snapshot: id, operation, timestamp
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers \
  | jq -r '.metadata.snapshots[] | [."snapshot-id", .summary.operation, ."timestamp-ms"] | @tsv'

# Which snapshot was current when (the table's history)
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers \
  | jq '.metadata["snapshot-log"]'
```

### Monitoring Commits

```bash
# Every commit writes a new metadata file; metadata-log lists the previous ones
curl -s -H "Authorization: Bearer $TOKEN" $CAT/namespaces/tutorial/tables/customers \
  | jq '.metadata["metadata-log"]'
```

Response:

```json
[
  {
    "timestamp-ms": 1704067200000,
    "metadata-file": "s3://warehouse/iceberg/tutorial/customers-<uuid>/metadata/00000-<uuid>.metadata.json"
  },
  {
    "timestamp-ms": 1704067300000,
    "metadata-file": "s3://warehouse/iceberg/tutorial/customers-<uuid>/metadata/00001-<uuid>.metadata.json"
  }
]
```

Property-only commits (like the ones in Part 3) add a metadata file but no
snapshot, so `metadata-log` usually has more entries than `snapshots`.

The same information is available as SQL: `lakehouse.tutorial.customers.snapshots`
/ `.history` in Spark, `iceberg.tutorial."customers$snapshots"` in Trino.

### Detecting Transaction Issues

A failed or conflicting commit never reaches Polaris's pointer, so the table
metadata stays consistent. What can be left behind are **orphan files**: data
files written by a writer whose commit then failed. They are never read, and
take up space but do no harm. (Iceberg's `remove_orphan_files` procedure is
meant for this, but on this stack it fails with `No FileSystem for scheme "s3"`:
it lists directories through Hadoop's file system, which the Spark image does
not configure for S3.)

### Common Issues and Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| Commit conflict | 409 `CommitFailedException` | Re-read the table and retry (engines do this automatically) |
| Expired token | 401 | Request a new token |
| Missing scope | 403 on every call | Request the token with `scope=PRINCIPAL_ROLE:ALL` |
| Wrong path | 404 | Include `/api/catalog` and the catalog name: `/api/catalog/v1/lakehouse/...` |
| Snapshot expiration | Missing snapshots | Increase retention |
| Permission denied | 403 `ForbiddenException` naming the operation | Check the grants on the principal's catalog roles |

---

## 🎯 Part 5: Access Control and Transactions

### Polaris Access Control Model

Polaris uses a role-based access control (RBAC) system:

```
┌──────────────────────────────────────────────────────────────────┐
│           Polaris Access Control                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐     ┌──────────────┐     ┌──────────────┐        │
│  │ Principal│────▶│ Principal    │────▶│ Catalog Role │        │
│  │ (user or │     │ Role         │     │ (per catalog)│        │
│  │  service)│     │              │     │              │        │
│  └──────────┘     └──────────────┘     └──────────────┘        │
│                                               │                 │
│                                               ▼                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Grants (Privileges) on a catalog, namespace or table      │  │
│  │                                                            │  │
│  │ Table: TABLE_LIST, TABLE_READ_DATA, TABLE_WRITE_DATA,     │  │
│  │        TABLE_CREATE, TABLE_DROP, TABLE_WRITE_PROPERTIES   │  │
│  │ Namespace: NAMESPACE_LIST, NAMESPACE_CREATE,              │  │
│  │            NAMESPACE_DROP                                 │  │
│  │ Catalog: CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_ACCESS    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

In this stack, the `root` principal has the `service_admin` principal role,
which is assigned the `catalog_admin` catalog role on `lakehouse`
(`CATALOG_MANAGE_CONTENT`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_ACCESS`):

```bash
MGMT=http://localhost:8181/api/management/v1
curl -s -H "Authorization: Bearer $TOKEN" $MGMT/principals
curl -s -H "Authorization: Bearer $TOKEN" $MGMT/principals/root/principal-roles
curl -s -H "Authorization: Bearer $TOKEN" $MGMT/catalogs/lakehouse/catalog-roles
curl -s -H "Authorization: Bearer $TOKEN" $MGMT/catalogs/lakehouse/catalog-roles/catalog_admin/grants
```

### Managing Transaction Permissions

Create a read-only service account for the `tutorial` namespace. All calls use
root's token against the management API:

```bash
# 1. Create a principal. The response contains its generated credentials --
#    the only time you see the secret.
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $MGMT/principals -d '{"principal": {"name": "data_reader"}}' | tee /tmp/data_reader.json
# {"principal":{"name":"data_reader","clientId":"32cf...",...},
#  "credentials":{"clientId":"32cf...","clientSecret":"ada6..."}}

# 2. Create a principal role and a catalog role
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $MGMT/principal-roles -d '{"principalRole": {"name": "reader_role"}}'
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $MGMT/catalogs/lakehouse/catalog-roles -d '{"catalogRole": {"name": "tutorial_reader"}}'

# 3. Grant read privileges on the tutorial namespace to the catalog role
for PRIV in NAMESPACE_LIST TABLE_LIST TABLE_READ_DATA; do
  curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    $MGMT/catalogs/lakehouse/catalog-roles/tutorial_reader/grants \
    -d "{\"grant\": {\"type\": \"namespace\", \"namespace\": [\"tutorial\"], \"privilege\": \"$PRIV\"}}"
done

# 4. Wire it together: catalog role -> principal role -> principal
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $MGMT/principal-roles/reader_role/catalog-roles/lakehouse \
  -d '{"catalogRole": {"name": "tutorial_reader"}}'
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  $MGMT/principals/data_reader/principal-roles \
  -d '{"principalRole": {"name": "reader_role"}}'
```

Each create returns `201`.

### Verifying Access Control

```bash
# Log in as data_reader with the generated credentials
READER_ID=$(jq -r .credentials.clientId /tmp/data_reader.json)
READER_SECRET=$(jq -r .credentials.clientSecret /tmp/data_reader.json)
READER_TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=$READER_ID -d client_secret=$READER_SECRET \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

# Read access
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $READER_TOKEN" \
  $CAT/namespaces/tutorial/tables
# Expected: 200 OK (read access granted)

# Try to commit to a table
curl -s -X POST -H "Authorization: Bearer $READER_TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables/customers \
  -d '{"requirements": [], "updates": [{"action": "set-properties", "updates": {"x": "y"}}]}'
# Expected: 403 Forbidden
# {"error":{"message":"Principal 'data_reader' ... is not authorized for op SET_TABLE_PROPERTIES",...}}

# Try to create a table
curl -s -X POST -H "Authorization: Bearer $READER_TOKEN" -H "Content-Type: application/json" \
  $CAT/namespaces/tutorial/tables \
  -d '{"name": "test", "schema": {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "int", "required": false}]}}'
# Expected: 403 Forbidden (... not authorized for op CREATE_TABLE_DIRECT)
```

Any other namespace is also `403` for `data_reader`, since the grants are only
on `tutorial`.

### Clean Up

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE -H "Authorization: Bearer $TOKEN" $MGMT/principals/data_reader
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE -H "Authorization: Bearer $TOKEN" $MGMT/principal-roles/reader_role
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE -H "Authorization: Bearer $TOKEN" $MGMT/catalogs/lakehouse/catalog-roles/tutorial_reader
# 204 each
```

---

## 📝 Summary

| Polaris Feature | ACID Relevance |
|----------------|---------------|
| **Metadata Locking** | Ensures atomic commits |
| **Snapshot Management** | Tracks transaction history |
| **Access Control** | Prevents unauthorized transactions |
| **Multi-Table Commits** | `transactions/commit` changes several tables atomically |
| **Credential Vending** | Short-lived storage credentials for clients (disabled in this stack: Garage has no STS) |

| REST API Endpoint | Purpose |
|-------------------|---------|
| `POST /api/catalog/v1/oauth/tokens` | Get a bearer token |
| `GET /api/catalog/v1/lakehouse/namespaces` | List namespaces |
| `GET /api/catalog/v1/lakehouse/namespaces/{ns}/tables` | List tables |
| `GET /api/catalog/v1/lakehouse/namespaces/{ns}/tables/{table}` | Load table metadata (snapshots, snapshot-log, metadata-log) |
| `POST /api/catalog/v1/lakehouse/namespaces/{ns}/tables/{table}` | Commit table updates |
| `POST /api/catalog/v1/lakehouse/transactions/commit` | Commit several tables atomically |
| `/api/management/v1/principals`, `/principal-roles`, `/catalogs/lakehouse/catalog-roles` | Manage access control |

---

## 🚀 Challenge Questions

1. **What happens if two processes try to update the same table simultaneously?**
2. **How does Polaris handle network partitions during commits?**
3. **Can you commit metadata without updating the current snapshot?**
4. **How does Polaris ensure durability of commits?**
5. **What's the difference between table-level and namespace-level access?**

---

## 📚 Additional Reading

- [Polaris Documentation](https://polaris.apache.org/docs/)
- [Iceberg REST Catalog Specification](https://iceberg.apache.org/rest-catalog-spec/)
- [Polaris Access Control](https://polaris.apache.org/docs/overview/access-control/)

---

**You've completed the Polaris ACID Transactions lab! Now you understand how Polaris enforces ACID guarantees for Iceberg tables across all engines.**