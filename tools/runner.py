"""Extract runnable python blocks from a lab README and execute them in order
in a single Spark session, mirroring what a learner does cell by cell.

Usage inside the jupyter container:
    python3 runner.py <abs-path-to-README.md> [skip_block_indices]
"""
import re
import sys
import traceback

README = sys.argv[1]
SKIP = {int(x) for x in sys.argv[2:]} if len(sys.argv) > 2 else set()

src = open(README).read()
blocks = re.findall(r"```(\w*)\n(.*?)```", src, re.S)

# Block 0 of every Spark lab is the canonical SparkSession snippet.
from pyspark.sql import SparkSession

creds = dict(l.strip().split("=", 1) for l in open("/creds/garage-credentials.env") if "=" in l)
spark = (
    SparkSession.builder.appName("lab-runner")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog")
    .config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")
    .config("spark.sql.catalog.lakehouse.credential", "root:root")
    .config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://objectstore:3900")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", creds["AWS_ACCESS_KEY_ID"])
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", creds["AWS_SECRET_ACCESS_KEY"])
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    .config("spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled", "false")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

GLOBALS = {"spark": spark, "__name__": "__main__"}
for _mod in ("time", "Thread", "ThreadPoolExecutor", "Path", "datetime"):
    pass  # blocks import what they need themselves

print(f"### {README}")
print(f"### session up, Spark {spark.version}\n")

failures = []
ran = 0
idx = -1
for lang, body in blocks:
    idx += 1
    if lang != "python":
        continue
    if idx in SKIP:
        print(f"--- block {idx}: SKIPPED ---")
        continue
    if "SparkSession.builder" in body:
        print(f"--- block {idx}: session snippet (already applied) ---")
        continue
    # Blockquote-wrapped blocks ("> ") are excerpts meant to be read, not pasted.
    if re.search(r"^\s*>", body, re.M):
        print(f"--- block {idx}: SKIPPED (blockquote excerpt) ---")
        continue
    # Illustrative blocks are marked by ellipsis placeholders.
    if re.search(r"\(\.\.\.\)|^\s*\.\.\.\s*$", body, re.M):
        print(f"--- block {idx}: SKIPPED (illustrative) ---")
        continue
    ran += 1
    print(f"--- block {idx} ---")
    try:
        # One shared namespace, so later blocks can use names earlier ones set
        # (first_snapshot_id, as_of, snapshots, ...), as a notebook would.
        exec(compile(body, f"<block {idx}>", "exec"), GLOBALS)
    except Exception as e:
        msg = str(e).strip().splitlines()[0][:200] if str(e).strip() else repr(e)
        print(f"    !! FAILED: {type(e).__name__}: {msg}")
        failures.append((idx, f"{type(e).__name__}: {msg}"))

print(f"\n### {README}: ran {ran} blocks, {len(failures)} failures")
for i, m in failures:
    print(f"    block {i}: {m}")

spark.stop()
sys.exit(1 if failures else 0)
