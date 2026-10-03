# Apache Iceberg Tutorial - Complete Directory Structure

```
iceberg-examples/
├── README.md                          # Main documentation
├── DIRECTORY.md                       # This file
├── docker-compose.yml                 # Service configuration (the whole stack)
├── .env.example                       # Optional overrides; copy to .env
├── config/                            # Configuration mounted into the containers
│   ├── jupyter/
│   │   └── Dockerfile                 # JupyterLab + PySpark image with Iceberg jars
│   ├── objectstore/
│   │   ├── garage.toml                # Garage (object store) config
│   │   ├── Dockerfile.bootstrap       # Image for the one-shot bootstrap container
│   │   └── bootstrap.sh               # Creates the bucket and the access key
│   ├── polaris/
│   │   ├── entrypoint.sh              # Loads the S3 key, starts Polaris
│   │   ├── bootstrap.sh               # Creates the `lakehouse` catalog
│   │   └── postgres-schema.sql        # Polaris tables in PostgreSQL
│   └── trino/
│       ├── entrypoint.sh              # Renders the catalog file, starts Trino
│       └── catalog/
│           └── iceberg.properties.template  # Trino `iceberg` catalog config
├── notebooks/                         # Mounted into JupyterLab; save your notebooks here
├── lab0-setup/                        # Setup Lab
│   ├── README.md                      # Lab 0 guide
│   ├── docker-compose.md              # Containers, ports and volumes explained
│   ├── startup.sh                     # Start the stack and wait until it is ready
│   └── spark-init.sql                 # Creates the `tutorial` namespace + sample tables
├── lab1-acid-transactions/            # Lab 1: ACID Transactions
│   ├── README.md                      # Lab 1 overview
│   ├── spark/
│   │   └── README.md                  # Lab 1 Spark guide
│   ├── trino/
│   │   └── README.md                  # Lab 1 Trino guide
│   └── polaris/
│       └── README.md                  # Lab 1 Polaris guide
├── lab2-time-travel/                  # Lab 2: Time Travel
│   └── README.md                      # Lab 2 guide
├── lab3-schema-evolution/             # Lab 3: Schema Evolution
│   └── README.md                      # Lab 3 guide
├── bonus-performance/                 # Bonus Lab: Performance
│   └── README.md                      # Bonus lab guide
└── common/                            # Shared resources
    ├── connection-reference.md        # Endpoints, credentials, Spark session snippet
    ├── glossary.md                    # Iceberg terms
    ├── architecture.md                # Architecture reference
    └── troubleshooting.md             # Common issues
```

## 📚 How to Use This Structure

1. **Start with the main README.md** for overview
2. **Run lab0-setup/startup.sh** to set up your environment
3. **Keep common/connection-reference.md open** - it has every endpoint, name and credential
4. **Work through labs in order**:
   - Lab 0: Setup (lab0-setup/)
   - Lab 1: ACID Transactions
   - Lab 2: Time Travel
   - Lab 3: Schema Evolution
   - Bonus Lab: Performance
5. **Lab 1 has three implementations**, one guide each:
   - Spark (writes/ETL)
   - Trino (queries)
   - Polaris (catalog)

   Labs 2, 3 and the bonus lab are a single README each.

## 🎯 Learning Path

```
┌──────────────────────────────────────────────────────────────┐
│                    Learning Journey                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Setup Your Environment (Lab 0)                          │
│     ↓                                                         │
│  2. Understand ACID (Lab 1)                                   │
│     ↓                                                         │
│  3. Query History (Lab 2)                                     │
│     ↓                                                         │
│  4. Evolve Schemas (Lab 3)                                    │
│     ↓                                                         │
│  5. Optimize (Bonus Lab)                                      │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

**Welcome to your Apache Iceberg journey! 🚀**