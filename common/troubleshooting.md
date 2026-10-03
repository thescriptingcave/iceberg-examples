# Iceberg Troubleshooting Guide

Names, endpoints and credentials used below are explained in the
[Connection Reference](connection-reference.md). Run the `docker compose`
commands from the repository root.

Most Polaris `curl` commands need a bearer token first:

```bash
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=root \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
```

## Common Issues and Solutions

### Issue 1: Connection Refused / Services Not Running

**Symptoms:**
- "Connection refused" error when connecting to services
- Services not starting
- Port conflicts

**Solutions:**

1. **Check if Docker is running:**
```bash
docker ps
```

2. **Check if services are running** (`-a` also shows the one-shot containers):
```bash
docker compose ps -a
```
Healthy looks like this. The two `*-bootstrap` containers are *supposed* to
have exited -- with code `0`. Any other exit code means that step failed.
```
SERVICE                 STATUS
jupyter                 Up 7 minutes (healthy)
objectstore             Up 42 minutes
objectstore-bootstrap   Exited (0) 3 minutes ago
polaris                 Up 12 minutes (healthy)
polaris-bootstrap       Exited (0) 3 minutes ago
postgres                Up 42 minutes (healthy)
trino                   Up 13 minutes (healthy)
```

3. **Check logs:**
```bash
docker compose logs <service-name>     # e.g. docker compose logs polaris-bootstrap
```

4. **Wait for services to start:**
Trino, Polaris and Jupyter only start after both bootstrap containers have
finished, so the first `docker compose up` can take a minute or two.

5. **Check port conflicts:**
```bash
lsof -i :8888 -i :8080 -i :8181 -i :3900 -i :5432
```

### Issue 2: Object-Store Credentials Never Appear

**Symptoms:**
- `trino` or `polaris` exits shortly after start
- Their logs end with `[trino-entrypoint] ERROR: credentials never appeared` or
  `[polaris-entrypoint] ERROR: /creds/garage-credentials.env never appeared`
- `objectstore-bootstrap` shows a non-zero exit code

**Cause:** Garage generates its access key on first boot, and
`objectstore-bootstrap` publishes it to `/creds/garage-credentials.env` on a
shared volume. Polaris, Trino and Jupyter wait up to two minutes for that file.
If the bootstrap failed, the file is never written.

**Solutions:**

1. **Read the bootstrap log** -- every step is logged with a `[garage-bootstrap]` prefix:
```bash
docker logs iceberg-objectstore-bootstrap
```
A good run ends with `credentials written to /creds/garage-credentials.env`
and `bootstrap complete`.

2. **Check that Garage itself is up and has a layout:**
```bash
docker exec iceberg-objectstore /garage status
```

3. **Check the credentials file** (works even when Trino/Polaris are down):
```bash
docker run --rm -v iceberg-tutorial_garage-creds:/creds:ro postgres:16-alpine ls -l /creds
```

4. **Re-run the bootstrap, then the services that depend on it.** It is idempotent:
```bash
docker compose up -d
```

> Never hardcode the key anywhere. It is generated per installation; read it
> from `/creds/garage-credentials.env` (see the
> [Connection Reference](connection-reference.md#object-store-credentials)).

### Issue 3: Polaris Realm Not Bootstrapped

**Symptoms:**
- Requesting a token with `root`/`root` returns HTTP 401
  `{"error":"unauthorized_client",...}`
- Spark, Trino and `curl` all fail to authenticate
- `polaris-bootstrap` shows a non-zero exit code

**Cause:** The Polaris server does **not** create its realm or root principal
on startup. `polaris-bootstrap` does that (realm `POLARIS`, principal
`root`/`root`) and then creates the `lakehouse` catalog. Until it has
succeeded there is nothing to log in as.

**Solutions:**

1. **Read the bootstrap log:**
```bash
docker logs iceberg-polaris-bootstrap
```
A good run says either `Realm 'POLARIS' is already bootstrapped; skipping.`
or that it bootstrapped it, and ends with `bootstrap complete`.

2. **Re-run it** (idempotent -- safe at any time):
```bash
docker compose up polaris-bootstrap
```

3. **Don't send a different realm.** Requests without a `Polaris-Realm` header
go to the default realm `POLARIS`. Any other realm name gets HTTP 404
`Missing or invalid realm`.

### Issue 4: Catalog Creation Returns 400

**Symptoms:** `POST /api/management/v1/catalogs` (by `polaris-bootstrap`, or
by you when creating an extra catalog) returns HTTP 400.

**Causes and fixes:**

1. **A field has the wrong JSON type.** `pathStyleAccess` (and
   `stsUnavailable`) must be JSON booleans: `"pathStyleAccess": true`. A value
   Polaris cannot read as a boolean is rejected with a 400 and an **empty
   response body**, so there is no message to tell you which field it was.

2. **The location overlaps an existing catalog.**
```json
{"error":{"message":"Cannot create Catalog scratch. One or more of its locations overlaps with an existing catalog","type":"ValidationException","code":400}}
```
The `lakehouse` catalog owns all of `s3://warehouse` (its `allowedLocations`).
A second catalog needs a location outside it, e.g. a different bucket.

To see what the existing catalog uses:
```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/management/v1/catalogs/lakehouse
```

### Issue 5: Trino Says "Cannot obtain metadata"

**Symptoms:**
- Every query against the `iceberg` catalog fails with
  `Query ... failed: Cannot obtain metadata`
- `SHOW SCHEMAS FROM iceberg` fails the same way

**Cause:** When the catalog is first used, Trino fetches
`<uri>/v1/config?warehouse=<warehouse>` from Polaris. If that returns 404,
Trino only reports "Cannot obtain metadata". The two usual reasons:

1. **The URI is missing the `/api/catalog` prefix.** It must be
   `http://polaris:8181/api/catalog`, not `http://polaris:8181`.
2. **`warehouse` is an S3 path instead of the catalog name.** For Polaris,
   `iceberg.rest-catalog.warehouse` must be `lakehouse`, not `s3://warehouse`.

**Solutions:**

1. **Check what Trino is actually using** (the rendered file, not the template):
```bash
docker exec iceberg-trino grep -E '^iceberg\.rest-catalog\.(uri|warehouse)' /etc/trino/catalog/iceberg.properties
```
Expected:
```
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=lakehouse
```

2. **Ask Polaris the same question Trino asks:**
```bash
# correct -> HTTP 200
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8181/api/catalog/v1/config?warehouse=lakehouse"
# missing prefix -> 404 "HTTP 404 Not Found"
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8181/v1/config?warehouse=lakehouse"
# S3 path as warehouse -> 404 "Unable to find warehouse s3://warehouse"
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8181/api/catalog/v1/config?warehouse=s3://warehouse"
```

3. **Fix `config/trino/catalog/iceberg.properties.template`** (never the
   rendered copy -- it is overwritten on every start), then:
```bash
docker compose restart trino
```

### Issue 6: "Catalog 'iceberg' failed to initialize"

**Symptoms:**
- Queries fail with `Catalog 'iceberg' failed to initialize and is disabled`
  (or the catalog is missing from `SHOW CATALOGS`)
- The Trino server itself is up

**Cause:** Trino could not load the catalog file at startup, so it disabled the
catalog instead of refusing to start. The usual cause is a property name Trino
doesn't know, which it reports as
`Configuration property '<name>' was not used`. Typical mistakes:
`fs.s3.enabled` (the real name is `fs.native-s3.enabled`) and
`iceberg.rest-catalog.oauth2-server-uri` (the real name has a dot:
`oauth2.server-uri`).

**Solutions:**

1. **Find the real error in the Trino log:**
```bash
docker logs iceberg-trino 2>&1 | grep -iE "failed to initialize|was not used|Configuration property"
```

2. **Check that the entrypoint rendered the file:**
```bash
docker logs iceberg-trino 2>&1 | grep trino-entrypoint
```
You should see `reading credentials`, `rendering /etc/trino/catalog/iceberg.properties`
and `starting trino`.

3. **Fix the template and restart Trino** (as in Issue 5).

### Issue 7: Polaris Returns 401 Unauthorized

**Symptoms:**
- `curl` calls to Polaris return HTTP 401 with an empty body
- A token request returns `invalid_scope` or `unauthorized_client`

**Solutions:**

1. **Token expired.** Tokens are valid for one hour (`"expires_in": 3600`).
   Request a new one (see the top of this page). Spark and Trino refresh their
   own tokens automatically; this only affects tokens you fetched by hand.

2. **No token at all.** Polaris has no basic auth: `curl -u root:root ...`
   gets a 401. Send `-H "Authorization: Bearer $TOKEN"`.

3. **Token request without a scope** returns
   `{"error":"invalid_scope","error_description":"The scope is invalid",...}`.
   Always pass `scope=PRINCIPAL_ROLE:ALL`.

4. **Wrong client secret** returns `unauthorized_client`. The credentials are
   `root`/`root` (`POLARIS_CLIENT_ID` / `POLARIS_CLIENT_SECRET`). If they are
   right and it still fails, see Issue 3.

### Issue 8: Permission Denied (403)

**Symptoms:**
- HTTP 403 / `ForbiddenException` from Polaris, e.g.
  `Principal 'root' with activated PrincipalRoles '[service_admin]' and activated grants via '[service_admin, catalog_admin]' is not authorized for op DROP_TABLE_WITH_PURGE`
- `DROP TABLE` from Trino fails, or writes fail with a message about missing
  `TABLE_WRITE_DATA`

**Causes:**

1. **Missing `CATALOG_MANAGE_CONTENT`.** The root principal reaches the
   `lakehouse` catalog through the catalog role `catalog_admin`, which by
   default may *manage* the catalog but not read/write table data or purge it.
   `polaris-bootstrap` grants `CATALOG_MANAGE_CONTENT` to fix that.
2. **Drop-with-purge disabled.** Trino's `DROP TABLE` always asks Polaris to
   purge the table's files, which Polaris refuses unless the catalog property
   `polaris.config.drop-with-purge.enabled` is `true`. `polaris-bootstrap`
   sets it.
3. **Token without `scope=PRINCIPAL_ROLE:ALL`** -- no roles are active, so
   nothing is allowed.

**Solutions:**

1. **Check the grants:**
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants
```
Expected to include `CATALOG_MANAGE_CONTENT`.

2. **Check the catalog properties:**
```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/management/v1/catalogs/lakehouse
```
Expected to include `"polaris.config.drop-with-purge.enabled": "true"` and
`"stsUnavailable": true`.

3. **Re-run the bootstrap**, which re-applies both:
```bash
docker compose up polaris-bootstrap
```

> `ForbiddenException ... REPORT_WRITE_METRICS` / `REPORT_READ_METRICS` lines in
> `docker logs iceberg-trino` are harmless: Trino tries to send query metrics
> to Polaris, Polaris refuses, and the query itself succeeds. (The Spark
> session turns this off with `rest-metrics-reporting-enabled=false`.)

### Issue 9: Garage Rejects Writes with "Invalid payload signature"

**Symptoms:**
- Creating a table or writing data fails with an S3 error containing
  `Invalid payload signature`
- Reads may still work

**Cause:** AWS SDK for Java v2 (2.30 and later) adds streaming CRC checksums to
every upload by default. Garage does not accept those and rejects the request.

**Solution:** Only send checksums when an operation strictly requires them, by
setting these environment variables on the client:

```
AWS_REQUEST_CHECKSUM_CALCULATION=when_required
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
```

`docker-compose.yml` already sets them for `polaris` and `jupyter`. If you see
this error, check that they are present in the container that failed, e.g.:

```bash
docker exec iceberg-jupyter env | grep CHECKSUM
```

and set the same variables for any other S3 client you point at Garage.

### Issue 10: JupyterLab Asks for a Token

**Symptoms:** http://localhost:8888 shows a "Password or token" login page.

**Solution:** The token is printed in the container log:
```bash
docker logs iceberg-jupyter 2>&1 | grep 'token=' | tail -1
```
Open the URL it prints (replace `127.0.0.1` with `localhost` if needed), or
paste the part after `token=` into the login page. The token changes every
time the container restarts; `./lab0-setup/startup.sh` always prints the
current login URL.

**The token is rejected even though it is current?** Another JupyterLab on
your machine is probably holding port 8888, and on macOS the browser reaches
*that* server instead of this one -- Docker reports no error. `startup.sh`
checks for this and stops with `ERROR: port 8888 is already used by: ...`.
Stop the other Jupyter, or move this one to another port:
```bash
cp -n .env.example .env        # if you have no .env yet
# then set JUPYTER_PORT=8889 in .env, and:
./lab0-setup/startup.sh        # now at http://localhost:8889
```

### Issue 11: `No module named 'pyspark'`

**Symptoms:** `from pyspark.sql import SparkSession` fails with
`ModuleNotFoundError: No module named 'pyspark'`.

**Cause:** PySpark ships inside `$SPARK_HOME` rather than as a Python package,
so the image puts it on `PYTHONPATH` (`config/jupyter/Dockerfile`). You get
this error if:
- the image is an old build without that `ENV` line, or
- you run a different Python than the notebook kernel, e.g. the OS's
  `/usr/bin/python3` (3.10) instead of conda's `python3` (3.11).

**Solutions:**

1. **Check from a terminal in JupyterLab (or via `docker exec`):**
```bash
docker exec iceberg-jupyter python3 -c "import pyspark; print(pyspark.__version__)"   # 3.5.0
docker exec iceberg-jupyter bash -c 'echo $PYTHONPATH'
```

2. **Rebuild the image if `PYTHONPATH` is empty:**
```bash
docker compose build jupyter && docker compose up -d jupyter
```

3. **Use the default `Python 3 (ipykernel)` kernel** in notebooks, and plain
   `python3` (not `/usr/bin/python3`) in terminals.

### Issue 12: Namespace Not Found

**Symptoms:**
- Spark: `NoSuchNamespaceException: Namespace does not exist: tutorial`, or
  `[TABLE_OR_VIEW_NOT_FOUND]` for a table in a missing namespace
- Trino: `Schema tutorial not found`

**Solutions:**

1. **Create the namespace first** (Lab 0 does this):
```sql
-- Spark
CREATE NAMESPACE IF NOT EXISTS lakehouse.tutorial;
-- Trino (Trino calls namespaces "schemas")
CREATE SCHEMA IF NOT EXISTS iceberg.tutorial;
```

2. **Check existing namespaces:**
```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces
```

3. **Verify namespace in catalog:**
```sql
SHOW NAMESPACES IN lakehouse;     -- Spark
SHOW SCHEMAS FROM iceberg;        -- Trino
```

### Issue 13: Commit Conflict

**Symptoms:**
- 409 Conflict error
- `CommitFailedException: Commit failed: Requirement failed: branch main has changed: expected id ... != ...`
- `... because it was concurrently modified`

**Solutions:**

1. **Understand the cause:** another writer committed between your read and
   your commit. Iceberg engines already retry automatically (4 times by
   default); you only see the error when all retries lose.

2. **Get the current snapshot ID:**
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['current-snapshot-id'])"
```

3. **Allow more automatic retries for a busy table:**
```python
spark.sql("ALTER TABLE lakehouse.tutorial.customers SET TBLPROPERTIES ('commit.retry.num-retries' = '10')")
```

4. **Implement retry logic:**
```python
import time

def safe_write(retries=3):
    for attempt in range(retries):
        try:
            # Perform write
            spark.sql("INSERT INTO lakehouse.tutorial.customers VALUES (...)")
            break
        except Exception as e:
            if "CommitFailedException" in str(e) or "conflict" in str(e).lower():
                print(f"Attempt {attempt+1}: Conflict, retrying...")
                time.sleep(1)
            else:
                raise
```

### Issue 14: Snapshot Expired

**Symptoms:**
- `Cannot find snapshot with ID ...` (Spark `VERSION AS OF`)
- `Cannot find a snapshot older than ...` (Spark `TIMESTAMP AS OF`)
- Historical data unavailable

**Solutions:**

1. **Get available snapshots:**
```python
spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at DESC").show()
```
or, in Trino:
```sql
SELECT snapshot_id, committed_at, operation FROM iceberg.tutorial."customers$snapshots" ORDER BY committed_at DESC;
```

2. **Check snapshot retention settings** (`history.expire.*` table properties):
```python
spark.sql("SHOW TBLPROPERTIES lakehouse.tutorial.customers").show(truncate=False)
```

3. **Increase retention period.** These are the defaults `expire_snapshots`
   uses when you don't pass explicit arguments:
```python
spark.sql("""
ALTER TABLE lakehouse.tutorial.customers SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '604800000',  -- 7 days
    'history.expire.min-snapshots-to-keep' = '10'
)
""")
```

4. **Remember:** a timestamp before the table's first snapshot is also
   "not found" -- time travel cannot go back further than the table exists.

### Issue 15: Small Files Issue

**Symptoms:**
- Slow queries
- High IO overhead
- Many small files (`.files` shows hundreds of files of a few KB)

**Solutions:**

1. **Check file sizes:**
```python
spark.sql("""
SELECT 
    file_path,
    file_size_in_bytes,
    record_count
FROM lakehouse.tutorial.customers.files
WHERE file_size_in_bytes < 10000000  -- < 10MB
""").show()
```

2. **Run compaction.** Compaction is run by an engine, not through a Polaris
   endpoint:
```python
spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.tutorial.customers')").show()
```
```sql
-- Trino
ALTER TABLE iceberg.tutorial.customers EXECUTE optimize;
```
Note that compaction never merges files *across* partitions. A table
partitioned by a high-cardinality column (like `customer_id`) stays one file
per partition -- see Issue 16.

3. **Schedule regular compaction:**
```python
import schedule
import time

def run_compaction():
    spark.sql("""
    CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.tutorial.customers')
    """)

# Run compaction daily
schedule.every().day.at("02:00").do(run_compaction)
```
(`schedule` is a third-party package, not installed in the image:
`pip install schedule`.)

See the [Performance lab](../bonus-performance/README.md) for sort/z-order
compaction and snapshot cleanup.

### Issue 16: Over-Partitioning

**Symptoms:**
- Too many partitions (>10,000)
- Slow metadata operations
- File system overhead

**Solutions:**

1. **Check partition count** (`.partitions` has one row per partition):
```python
spark.sql("SELECT COUNT(*) FROM lakehouse.tutorial.customers.partitions").show()
```

2. **Create new table with coarser partitioning:**
```python
# Current: one partition per customer_id (far too granular)
# New: daily partitioning (more reasonable)

spark.sql("""
CREATE TABLE lakehouse.tutorial.customers_daily_partition
USING ICEBERG
PARTITIONED BY (days(created_at))
AS SELECT customer_id, name, email, created_at
FROM lakehouse.tutorial.customers
""")
```

3. **Or evolve the partitioning in place** (new data uses the new spec, existing
   files keep theirs until rewritten):
```python
spark.sql("ALTER TABLE lakehouse.tutorial.customers REPLACE PARTITION FIELD customer_id WITH bucket(16, customer_id)")
```

### Issue 17: Schema Mismatch

**Symptoms:**
- Column not found error
- Type mismatch error
- Schema evolution issues

**Solutions:**

1. **Check current schema:**
```sql
DESCRIBE lakehouse.tutorial.customers;
```

2. **Get schema history** (which schema each metadata version used):
```python
spark.sql("""
SELECT timestamp, file, latest_snapshot_id, latest_schema_id
FROM lakehouse.tutorial.customers.metadata_log_entries
""").show(truncate=False)
```

3. **Use column projection:**
```python
# Read only columns that exist
df = spark.read.format("iceberg").load("lakehouse.tutorial.customers")
df.select("customer_id", "name").show()
```

4. **Handle schema changes gracefully:**
```python
try:
    df = spark.read.format("iceberg").load("lakehouse.tutorial.customers")
    # Check if column exists
    if 'phone' in df.columns:
        df.select("customer_id", "phone").show()
    else:
        df.select("customer_id").show()
except Exception as e:
    print(f"Error: {e}")
```

### Issue 18: Data Not Visible

**Symptoms:**
- Data inserted but not visible in queries
- Stale queries
- Old data showing

**Solutions:**

1. **Check snapshot:**
```python
spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.tutorial.customers.snapshots ORDER BY committed_at DESC").show()
```

2. **Verify current snapshot:**
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['current-snapshot-id'])"
```

3. **Refresh table:** Spark's Iceberg catalog caches loaded tables for a short
   time, so a write made from Trino may not show up in Spark immediately.
```python
spark.sql("REFRESH TABLE lakehouse.tutorial.customers")
```

4. **Check time travel:**
```python
# Query by timestamp
spark.sql("""
SELECT * FROM lakehouse.tutorial.customers 
TIMESTAMP AS OF '2024-01-01 12:00:00'
""").show()
```

---

## Diagnostic Commands

### Check Service Status
```bash
# Check all services, including the one-shot bootstrap containers
docker compose ps -a

# Check specific service
docker compose ps -a <service-name>

# Check logs
docker compose logs <service-name>
docker logs iceberg-objectstore-bootstrap
docker logs iceberg-polaris-bootstrap
```

### Check Catalog Connectivity
```bash
# Check Polaris is listening (401 without a token is expected and means "up")
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/api/catalog/v1/config

# Same check from inside the Docker network
docker exec iceberg-trino curl -s -o /dev/null -w '%{http_code}\n' http://polaris:8181/api/catalog/v1/config

# Check catalog config (needs a token, see top of page)
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8181/api/catalog/v1/config?warehouse=lakehouse"

# Check catalog list and namespaces
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/management/v1/catalogs
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces

# Check table metadata
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers
```

### Check Storage
```bash
# Garage node status (there is no web console)
docker exec iceberg-objectstore /garage status

# Bucket size, object count and which key may access it
docker exec iceberg-objectstore /garage bucket info warehouse

# The generated access key
docker exec iceberg-objectstore /garage key list
```

### Check Table Health
```python
# Check file statistics
spark.sql("""
SELECT 
    count(*) as total_files,
    min(file_size_in_bytes) as min_size,
    avg(file_size_in_bytes) as avg_size,
    max(file_size_in_bytes) as max_size
FROM lakehouse.tutorial.customers.files
""").show()

# Check partition statistics (partitioned tables only)
spark.sql("""
SELECT 
    partition,
    count(*) as file_count,
    sum(file_size_in_bytes) as total_size
FROM lakehouse.tutorial.customers.files
GROUP BY partition
ORDER BY file_count DESC
""").show()

# Check snapshot history
spark.sql("""
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['added-data-files'] AS added_files,
    summary['added-records'] AS added_records
FROM lakehouse.tutorial.customers.snapshots
ORDER BY committed_at DESC
LIMIT 10
""").show()
```

---

## Environment Checklist

Before reporting an issue, verify:

- [ ] Docker is running
- [ ] All services are up and both bootstrap containers show `Exited (0)` (`docker compose ps -a`)
- [ ] `/creds/garage-credentials.env` exists (Issue 2)
- [ ] You can get a Polaris token with `root`/`root` and `scope=PRINCIPAL_ROLE:ALL`
- [ ] Polaris URIs include `/api/catalog`, and `warehouse` is `lakehouse`
- [ ] Namespace exists
- [ ] Table exists
- [ ] Current snapshot exists
- [ ] Storage is accessible
- [ ] Catalog is reachable

---

## Getting Help

1. **Check this guide first** - many issues are common and documented here
2. **Check logs** - they often contain the root cause (`docker logs <container>`)
3. **Verify environment** - use the checklist above
4. **Try minimal reproduction** - isolate the issue
5. **Check GitHub issues** - search for similar problems

---

**Remember: Most issues are configuration-related. Double-check your environment setup before assuming a bug!**
