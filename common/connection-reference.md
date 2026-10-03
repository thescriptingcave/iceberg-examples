# Connection Reference

The single source of truth for how to reach every part of the stack. Every lab
links here rather than repeating these details -- if something in a lab
disagrees with this page, this page wins.

## Names you will see everywhere

| Thing | Name | Where it is defined |
|---|---|---|
| Polaris catalog | `lakehouse` | `POLARIS_CATALOG` in `docker-compose.yml`, created by `config/polaris/bootstrap.sh` |
| Spark catalog alias | `lakehouse` | the `SparkSession` config below |
| Trino catalog | `iceberg` | `config/trino/catalog/iceberg.properties.template` |
| Namespace used by the labs | `tutorial` | created in Lab 0 |
| Object-store bucket | `warehouse` | `S3_BUCKET` in `docker-compose.yml` (override in `.env`) |
| Table data location | `s3://warehouse/iceberg/<namespace>/<table>/` (Spark) or `.../<table>-<uuid>/` (Trino) | Polaris's `default-base-location`; Trino appends a UUID so a re-created table never reuses old files |

So the same table is `lakehouse.tutorial.customers` in Spark and
`iceberg.tutorial.customers` in Trino.

## Endpoints

| Service | From your laptop | From inside the containers | Login |
|---|---|---|---|
| JupyterLab (Spark) | http://localhost:8888 (or `JUPYTER_PORT` from `.env`) | -- | token, see below |
| Spark UI | http://localhost:4040 | -- | only while a `SparkSession` is running |
| Trino web UI | http://localhost:8080 | `http://trino:8080` | any user name, no password |
| Polaris Iceberg REST API | http://localhost:8181/api/catalog | `http://polaris:8181/api/catalog` | OAuth2, `root` / `root` |
| Polaris management API | http://localhost:8181/api/management/v1 | `http://polaris:8181/api/management/v1` | OAuth2, `root` / `root` |
| Garage S3 API | http://localhost:3900 | `http://objectstore:3900` | generated key, see below |
| PostgreSQL (Polaris metadata) | `localhost:5432` | `postgres:5432` | `polaris` / `polaris` |

There is **no** web console for the object store and **no** Polaris admin UI.

### JupyterLab files

JupyterLab's file browser opens in the project's `notebooks/` folder, which is
mounted from your machine -- every notebook you save there is kept. (The path
inside the container is `/home/jovyan/notebooks`.)

### JupyterLab token

```bash
docker logs iceberg-jupyter 2>&1 | grep 'token=' | tail -1
```

## Object-store credentials

Garage generates its access key the first time it starts, so the key is not in
any file in this repo. The `objectstore-bootstrap` container writes it to a
shared volume, and Polaris, Trino and Jupyter read it from
`/creds/garage-credentials.env`:

```bash
docker exec iceberg-trino cat /creds/garage-credentials.env
```

To look at what is in the bucket:

```bash
docker exec iceberg-objectstore /garage bucket info warehouse
```

## Spark (in a Jupyter notebook)

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

| Setting | Why |
|---|---|
| `uri` ends in `/api/catalog` | Polaris serves the Iceberg REST API under that prefix. Without it you get 404s. |
| `warehouse` is `lakehouse` | For Polaris this is the **catalog name**, not an S3 path. |
| `scope=PRINCIPAL_ROLE:ALL` | Activates the root principal's roles; without it every call is 403. |
| `s3.path-style-access=true` | Garage addresses buckets as `host/bucket`, not `bucket.host`. |
| static S3 keys | Garage has no STS, so Polaris cannot vend temporary credentials. |
| `spark.sql.defaultCatalog` | Lets you write `tutorial.customers` instead of `lakehouse.tutorial.customers`. |

Metadata tables are addressed as `lakehouse.tutorial.customers.snapshots`
(also `.history`, `.files`, `.manifests`, `.partitions`, `.refs`).
Procedures are called as `CALL lakehouse.system.<procedure>(...)`.

## Trino

```bash
docker exec -it iceberg-trino trino
```

```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.tutorial.customers;
-- or: USE iceberg.tutorial;
```

Metadata tables are addressed with a `$` suffix and must be quoted:
`SELECT * FROM iceberg.tutorial."customers$snapshots"`.

The catalog file is generated at startup from
`config/trino/catalog/iceberg.properties.template` -- edit the template, then
`docker compose restart trino`.

## Polaris REST API

Every call needs a bearer token:

```bash
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=root \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
```

```bash
# Catalogs (management API)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/management/v1/catalogs

# Namespaces and tables (Iceberg REST API -- note the catalog name in the path)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/catalog/v1/lakehouse/namespaces/tutorial/tables/customers
```

Tokens expire after one hour; request a new one if you get a 401.
