# Apache Iceberg Tutorial

A comprehensive, verbose, and informative tutorial series for learning Apache Iceberg concepts through hands-on labs using Spark, Trino, and Polaris.

## 🎯 What You'll Learn

This tutorial is designed for **complete beginners** with no prior data engineering or distributed systems experience. We'll build your understanding step by step, explaining not just *how* to do things, but *why* they work the way they do.

### Core Concepts Covered

1. **ACID Transactions** - Learn how distributed systems maintain data consistency even when multiple operations happen simultaneously
2. **Time Travel** - Query historical versions of data and understand how versioning works in modern data lakes
3. **Schema Evolution** - Modify table structures over time without breaking existing queries or data
4. **Catalog Architecture** - Understand the role of Polaris as the metadata coordinator

### Tools You'll Master

| Tool | What It Does | Why We Use It |
|------|--------------|---------------|
| **Spark** | Data processing engine | Industry standard for ETL and complex transformations; native Iceberg support |
| **Trino** | Distributed SQL query engine | Fast, interactive queries across multiple data sources; perfect for learning |
| **Polaris** | Iceberg REST catalog | Open-source catalog with fine-grained access control; the "glue" that enables interoperability |
| **Garage** | Object storage | Lightweight S3-compatible storage for learning without cloud costs |

---

## 📚 Documentation Structure

```
iceberg-examples/
├── README.md                          # This file
├── DIRECTORY.md                       # Directory structure overview
├── docker-compose.yml                 # Service configuration
├── .env.example                       # Optional overrides; copy to .env
├── config/                            # Per-service config (Garage, Polaris, Trino, Jupyter)
├── notebooks/                         # Mounted into JupyterLab
│
├── lab0-setup/                        # Lab 0: Environment Setup
│   ├── README.md                      # Environment setup guide
│   ├── docker-compose.md              # Containers, ports and volumes explained
│   ├── startup.sh                     # Start the stack and wait until it is ready
│   └── spark-init.sql                 # Create the `tutorial` namespace + sample tables
│
├── lab1-acid-transactions/            # Lab 1: ACID Transactions
│   ├── README.md                      # Lab overview
│   ├── spark/                         # Spark implementation
│   │   └── README.md                  # Spark-specific guide
│   ├── trino/                         # Trino implementation
│   │   └── README.md                  # Trino-specific guide
│   └── polaris/                       # Polaris implementation
│       └── README.md                  # Polaris-specific guide
│
├── lab2-time-travel/                  # Lab 2: Time Travel
│   └── README.md                      # Lab guide
│
├── lab3-schema-evolution/             # Lab 3: Schema Evolution
│   └── README.md                      # Lab guide
│
├── bonus-performance/                 # Bonus Lab: Performance
│   └── README.md                      # Lab guide
│
└── common/                            # Shared resources
    ├── connection-reference.md        # Endpoints, credentials, Spark session snippet
    ├── glossary.md                    # Iceberg terminology
    ├── architecture.md                # Architecture reference
    └── troubleshooting.md             # Common issues and solutions
```

---

## 🚀 Getting Started

### Prerequisites

- **Docker Desktop** (version 20.10 or higher)
  - Install: https://www.docker.com/get-started/
  
- **Docker Compose** (version 2.0 or higher)
  - Included with Docker Desktop

- **At least 8GB RAM** (16GB recommended for smooth operation)
  - Docker Desktop → Settings → Resources → Memory

### Quick Start (5 minutes)

1. **Go to the project directory** (wherever you cloned or unpacked it):
```bash
cd iceberg-examples
```

2. **Start all services and wait until they are ready:**
```bash
./lab0-setup/startup.sh
```
This runs `docker compose up -d` for you, polls each service until it answers (the very first start builds two images and can take several minutes), then prints the access points and your JupyterLab token. There is no need to run `docker compose up` first.

3. **Verify services are running:**
```bash
docker compose ps
```

You should see:
```
NAME                  SERVICE       STATUS
iceberg-jupyter       jupyter       Up (healthy)
iceberg-objectstore   objectstore   Up
iceberg-polaris       polaris       Up (healthy)
iceberg-postgres      postgres      Up (healthy)
iceberg-trino         trino         Up (healthy)
```
Two one-shot containers, `iceberg-objectstore-bootstrap` and `iceberg-polaris-bootstrap`, set things up and exit; `docker compose ps -a` should show them as `Exited (0)`.

4. **Access the services:**
- **JupyterLab (Spark)**: http://localhost:8888 (token: `docker logs iceberg-jupyter 2>&1 | grep 'token=' | tail -1`)
- **Trino**: http://localhost:8080 (any user name, no password)
- **Polaris REST API**: http://localhost:8181/api/catalog (OAuth2 client credentials `root` / `root`)
- **Garage S3 API**: http://localhost:3900 (access key generated at first boot)
- **Spark UI**: http://localhost:4040 (only while a `SparkSession` is running)

There is no object-store web console and no Polaris admin UI. Everything you need to connect -- catalog and namespace names, credentials, and the `SparkSession` snippet used in every lab -- is in the **[Connection Reference](common/connection-reference.md)**.

5. **Create the `tutorial` namespace** the labs use: see "Initialize the tutorial namespace" in [Lab 0](lab0-setup/README.md).

In Spark, tables are named `lakehouse.tutorial.<table>`; in Trino the same tables are `iceberg.tutorial.<table>`.

---

## 📖 Learning Path

### Lab 0: Environment Setup
**Time**: 30-60 minutes
**Prerequisites**: Docker, basic command line knowledge

**What you'll learn:**
- Understanding the Iceberg three-layer architecture
- Setting up your local development environment
- Knowing what each tool does (Spark, Trino, Polaris, Garage)
- Running your first queries

**Key concepts:**
- Object Storage (Garage)
- Catalog (Polaris)
- Compute Engines (Spark, Trino)
- Iceberg terminology

**Read**: [Lab 0 README](lab0-setup/README.md)

---

### Lab 1: ACID Transactions
**Time**: 2-3 hours
**Prerequisites**: Lab 0 completed

**What you'll learn:**
- Understanding ACID properties in distributed systems
- Creating Iceberg tables
- Performing transactions (INSERT, UPDATE, DELETE)
- Handling concurrent writes
- Verifying ACID guarantees

**Three approaches:**
- **Spark**: [Spark ACID Guide](lab1-acid-transactions/spark/README.md)
- **Trino**: [Trino ACID Guide](lab1-acid-transactions/trino/README.md)
- **Polaris**: [Polaris ACID Guide](lab1-acid-transactions/polaris/README.md)

**Key concepts:**
- Atomicity
- Consistency
- Isolation
- Durability
- Commit conflicts
- Optimistic locking

**Read**: [Lab 1 Overview](lab1-acid-transactions/README.md)

---

### Lab 2: Time Travel
**Time**: 2-3 hours
**Prerequisites**: Lab 1 completed

**What you'll learn:**
- Understanding versioning in Iceberg
- Querying historical versions
- Point-in-time recovery
- Comparing data across time

**Key concepts:**
- Snapshots
- Snapshot chain
- Timestamp queries
- Snapshot ID queries
- Time travel use cases

**Read**: [Lab 2 README](lab2-time-travel/README.md)

---

### Lab 3: Schema Evolution
**Time**: 2-3 hours
**Prerequisites**: Lab 2 completed

**What you'll learn:**
- Understanding schema evolution
- Adding columns
- Removing columns
- Updating column types
- Renaming columns
- Backward and forward compatibility

**Key concepts:**
- Schema evolution
- Type promotion
- Column projection
- Backward compatibility
- Forward compatibility

**Read**: [Lab 3 README](lab3-schema-evolution/README.md)

---

### Bonus Lab: Performance Optimization
**Time**: 2-3 hours
**Prerequisites**: Lab 3 completed

**What you'll learn:**
- Partitioning strategies
- File sizing
- Z-order indexing
- Compaction
- Monitoring table health

**Key concepts:**
- Partition pruning
- File compaction
- Z-order clustering
- Query optimization
- Performance metrics

**Read**: [Bonus Lab README](bonus-performance/README.md)

---

## 📚 Reference Materials

### Connection Reference
**Read**: [Connection Reference](common/connection-reference.md)

Every endpoint, name and credential in the stack, plus the `SparkSession` snippet. If a lab disagrees with it, the reference wins.

### Glossary
**Read**: [Iceberg Glossary](common/glossary.md)

Comprehensive list of Iceberg terms, concepts, and abbreviations.

### Architecture Reference
**Read**: [Iceberg Architecture](common/architecture.md)

Detailed architecture documentation, component details, and configuration reference.

### Troubleshooting
**Read**: [Troubleshooting Guide](common/troubleshooting.md)

Common issues and their solutions, plus diagnostic commands.

---

## 🛠️ Common Commands

### Service Management
```bash
# Start all services
docker compose up -d

# Check service status
docker compose ps

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

### Connect to Services
```bash
# Spark: open JupyterLab at http://localhost:8888 and start a notebook with the
# SparkSession from common/connection-reference.md. To get the login token:
docker logs iceberg-jupyter 2>&1 | grep 'token=' | tail -1

# Connect to Trino
docker exec -it iceberg-trino trino --catalog iceberg

# Connect to PostgreSQL
docker exec -it iceberg-postgres psql -U polaris -d polaris
```

### Common Tasks
```bash
# Check the object store was bootstrapped (prints "exited 0")
docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' iceberg-objectstore-bootstrap

# Check Polaris (401 = up; every real call needs a bearer token, see the Connection Reference)
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/api/catalog/v1/config

# Check Trino ("starting":false = ready)
curl -s http://localhost:8080/v1/info

# Show the generated object-store credentials
docker exec iceberg-trino cat /creds/garage-credentials.env
```

---

## 🎯 Success Checklist

By the end of this tutorial, you should be able to:

- [ ] Set up an Iceberg environment on your local machine
- [ ] Explain the three-layer architecture (Storage → Catalog → Compute)
- [ ] Create Iceberg tables using Spark
- [ ] Perform ACID transactions with Spark, Trino, and Polaris
- [ ] Query historical versions using time travel
- [ ] Evolve table schemas without breaking queries
- [ ] Optimize table performance with partitioning and compaction
- [ ] Debug common Iceberg issues

---

## 🔍 Understanding the Concepts

### The Iceberg Three-Layer Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    ICEBERG ARCHITECTURE                       │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              LAYER 1: Object Storage                    │ │
│  │               (Garage, S3, GCS, etc.)                   │ │
│  │                                                           │ │
│  │  Holds:                                                  │ │
│  │  • Parquet data files                                    │ │
│  │  • Manifest files (pointers to data)                    │ │
│  │  • Metadata files (table definitions)                   │ │
│  │  • Snapshot files (version pointers)                    │ │
│  │                                                           │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              ▲                                │
│                              │                                │
│  ┌───────────────────────────┼─────────────────────────────┐ │
│  │                           │                               │ │
│  │                  LAYER 2: CATALOG                        │ │
│  │               (Polaris, Glue, Hive, Nessie)             │ │
│  │                                                           │ │
│  │  Holds:                                                  │ │
│  │  • Table metadata (schema, properties)                  │ │
│  │  • Current snapshot pointer                             │ │
│  │  • Access control policies                              │ │
│  │  • Version history                                      │ │
│  │                                                           │ │
│  └───────────────────────────┼─────────────────────────────┘ │
│                              │                                │
│  ┌───────────────────────────▼─────────────────────────────┐ │
│  │                   LAYER 3: COMPUTE                       │ │
│  │                (Spark, Trino, Flink, etc.)              │ │
│  │                                                           │ │
│  │  Does:                                                   │ │
│  │  • Reads metadata from catalog                          │ │
│  │  • Fetches data files from storage                      │ │
│  │  • Processes and transforms data                        │ │
│  │  • Writes new data back to storage                      │ │
│  │                                                           │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### Why Three Layers?

| Layer | Why It's Separated | Benefit |
|-------|-------------------|---------|
| **Storage** | Cost-effective object storage (S3) | Scalable, durable, cheap |
| **Catalog** | Central metadata management | Consistent view, ACID, access control |
| **Compute** | Independent compute scaling | Scale compute separately from storage |

This separation enables:
- **Independent scaling**: Add more compute without touching storage
- **Multi-engine support**: Spark, Trino, Flink can all access the same data
- **Cost optimization**: Store data cheaply, compute only when needed
- **Flexibility**: Change compute engines without moving data

---

## 💡 Tips for Success

1. **Take your time** - This tutorial is verbose for a reason. Read everything carefully.
2. **Experiment** - Don't just follow along; try modifying the code.
3. **Break things** - Intentionally cause errors to understand error messages.
4. **Compare approaches** - See how Spark, Trino, and Polaris solve the same problem differently.
5. **Ask questions** - The "Common Pitfalls" sections are based on real issues.

---

## 📈 What You'll Build

By the end of this tutorial, you'll have:
- A working Iceberg environment on your local machine
- Understanding of core distributed systems concepts
- Ability to use Spark, Trino, and Polaris effectively
- Knowledge to apply Iceberg in real-world scenarios
- Confidence to explore advanced topics

---

## 🤝 Getting Help

If you get stuck:

1. **Check the troubleshooting guide**: [Troubleshooting](common/troubleshooting.md)
2. **Review the glossary**: [Glossary](common/glossary.md)
3. **Check the architecture**: [Architecture](common/architecture.md)
4. **Review the lab README** for your specific issue
5. **Try the common commands** section above

---

## 📜 License

This tutorial is open source and available under the Apache 2.0 License.

---

## 🙏 Acknowledgments

- Apache Iceberg community for the amazing open standard
- Polaris team for the open-source catalog implementation
- Spark and Trino communities for their excellent query engines
- Garage for the open-source object storage

---

## 📞 Community Resources

- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Polaris](https://polaris.apache.org/)
- [Trino](https://trino.io/)
- [Apache Spark](https://spark.apache.org/)
- [Garage](https://garagehq.deuxfleurs.fr/)

---

**Ready to begin?** Head to [Lab 0: Environment Setup](lab0-setup/README.md) to get started!

---

**Questions? Check the troubleshooting guide first - many issues are common and documented!**