# Lab 0: Docker Compose Setup

The Docker Compose configuration for your Iceberg tutorial environment lives in the project root (`docker-compose.yml`). This page summarises what it starts. For endpoints and credentials, see the [Connection Reference](../common/connection-reference.md).

## 📁 Files

| File | Purpose |
|------|---------|
| `../docker-compose.yml` | Main Docker Compose configuration |
| `../.env.example` | Optional overrides for the compose file; copy to `../.env` to use |
| `../config/objectstore/` | Garage config (`garage.toml`) and the bucket/key bootstrap script |
| `../config/polaris/` | Polaris entrypoint, PostgreSQL schema, and the catalog bootstrap script |
| `../config/trino/catalog/iceberg.properties.template` | Trino Iceberg catalog configuration (rendered at startup) |
| `../config/jupyter/Dockerfile` | JupyterLab + PySpark image with the Iceberg jars added |
| `spark-init.sql` | Creates the `tutorial` namespace and sample tables |

## 🚀 Quick Start

Run these from the project root:

```bash
# Start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

## 🌐 Access Points

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| JupyterLab (Spark) | 8888 | http://localhost:8888 | token: `docker logs iceberg-jupyter 2>&1 \| grep 'token=' \| tail -1` |
| Spark UI | 4040 | http://localhost:4040 | - (only while a `SparkSession` is running) |
| Trino | 8080 | http://localhost:8080 | any user name, no password |
| Polaris REST API | 8181 | http://localhost:8181/api/catalog | OAuth2 `root` / `root` |
| Polaris management API | 8181 | http://localhost:8181/api/management/v1 | OAuth2 `root` / `root` |
| Garage S3 API | 3900 | http://localhost:3900 | generated at first boot, see `/creds/garage-credentials.env` |
| PostgreSQL | 5432 | `localhost:5432` | `polaris` / `polaris` |

The compose file also publishes Garage's WebDAV (4883) and admin API (9899) ports; the labs do not use them. There is no object-store web console and no Polaris admin UI.

## 🐳 Docker Containers

| Container | Image | Purpose |
|-----------|-------|---------|
| `iceberg-objectstore` | dxflrs/garage:v2.1.0 | Object storage (S3 API) |
| `iceberg-objectstore-bootstrap` | iceberg-garage-bootstrap:local (built from `config/objectstore`) | One-shot: creates the `warehouse` bucket and access key |
| `iceberg-postgres` | postgres:16-alpine | Polaris backend database |
| `iceberg-polaris` | apache/polaris:1.8.0 | Iceberg REST catalog |
| `iceberg-polaris-bootstrap` | apache/polaris-admin-tool:1.8.0 | One-shot: creates the root principal and the `lakehouse` catalog |
| `iceberg-jupyter` | iceberg-jupyter:local (built from `config/jupyter`) | JupyterLab + Spark 3.5 with Iceberg 1.9.1 |
| `iceberg-trino` | trinodb/trino:477 | Trino query engine |

The two bootstrap containers exit when they are done; `docker compose ps -a` should show them as `Exited (0)`.

## 📊 Volume Mounts

| Volume | Purpose |
|--------|---------|
| `garage-meta` | Garage metadata (shared read-only with the bootstrap container) |
| `garage-data` | Garage object data |
| `postgres-data` | PostgreSQL database data |
| `garage-creds` | The generated S3 access key, read by Polaris, Trino and Jupyter at `/creds` |

In addition, `./notebooks` is mounted into JupyterLab at `/home/jovyan/notebooks`.

## 🛠️ Troubleshooting

### Services not starting

```bash
# Check Docker is running
docker ps

# Check status, including the one-shot bootstrap containers
docker compose ps -a

# Check logs
docker compose logs <service-name>
```

### Memory issues

Increase Docker memory allocation:
- Docker Desktop → Settings → Resources → Memory → 8GB+

### Network issues

Services reach each other by service name on the `iceberg-network` network. To check from inside the Jupyter container:

```bash
# Polaris: expect 401 (it is up, you just did not send a token)
docker exec iceberg-jupyter curl -s -o /dev/null -w '%{http_code}\n' http://polaris:8181/api/catalog/v1/config

# Garage: expect 403 (it is up, the request was not signed)
docker exec iceberg-jupyter curl -s -o /dev/null -w '%{http_code}\n' http://objectstore:3900
```

A `000` means the service cannot be reached.

---

**Next**: See [Lab 0 README](README.md) for full lab instructions.
