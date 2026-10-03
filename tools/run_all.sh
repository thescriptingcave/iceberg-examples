#!/bin/bash
# Top-to-bottom run of every lab, from an empty namespace, in the order a
# learner following the handout would work through them.
#
# Each engine gets a driver that replays the README's code blocks verbatim:
#   runner.py         (in jupyter)  python blocks, one shared Spark session
#   trino_runner.py                 sql blocks, USE re-issued per statement
#   polaris_runner.py               bash/curl blocks against the live stack
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUN="$HERE"
REPO=$(cd "$HERE/.." && pwd)
OUT=$RUN/full-run.log
: > "$OUT"

banner() { printf '\n\n========== %s ==========\n' "$*" | tee -a "$OUT"; }

banner "STEP 0: stage runner.py in the jupyter container, drop every tutorial table"
# runner.py executes inside the container, where pyspark lives; the READMEs are
# copied in per-lab. Both must exist before any step runs.
docker cp "$RUN/runner.py" iceberg-jupyter:/tmp/runner.py
for t in customers customers_evo customers_num customers_int_age \
         customers_string customers_age_int customers_newer \
         customers_compat customers_recovered customers_recovery \
         perf_customers partitioned_events events sales products orders; do
    docker exec iceberg-trino trino --execute "DROP TABLE IF EXISTS iceberg.tutorial.$t" >/dev/null 2>&1
done
docker exec iceberg-trino trino --execute "SHOW TABLES FROM iceberg.tutorial" 2>&1 \
    | grep -vE "WARNING|org.jline" | tee -a "$OUT"
echo "(nothing listed above = clean slate)" | tee -a "$OUT"

banner "STEP 1: Lab 0 -- the documented spark-init.sql cell"
docker exec -i -w /home/jovyan/notebooks iceberg-jupyter python3 -c "
from pyspark.sql import SparkSession
creds = dict(l.strip().split('=',1) for l in open('/creds/garage-credentials.env') if '=' in l)
spark = (SparkSession.builder.appName('lab0')
    .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse','org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type','rest')
    .config('spark.sql.catalog.lakehouse.uri','http://polaris:8181/api/catalog')
    .config('spark.sql.catalog.lakehouse.warehouse','lakehouse')
    .config('spark.sql.catalog.lakehouse.credential','root:root')
    .config('spark.sql.catalog.lakehouse.scope','PRINCIPAL_ROLE:ALL')
    .config('spark.sql.catalog.lakehouse.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
    .config('spark.sql.catalog.lakehouse.s3.endpoint','http://objectstore:3900')
    .config('spark.sql.catalog.lakehouse.s3.path-style-access','true')
    .config('spark.sql.catalog.lakehouse.s3.access-key-id',creds['AWS_ACCESS_KEY_ID'])
    .config('spark.sql.catalog.lakehouse.s3.secret-access-key',creds['AWS_SECRET_ACCESS_KEY'])
    .config('spark.sql.catalog.lakehouse.client.region','us-east-1')
    .config('spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled','false')
    .config('spark.sql.defaultCatalog','lakehouse')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
from pathlib import Path
sql_text = Path('spark-init.sql').read_text()
statements = [s.strip() for s in sql_text.split(';') if s.strip()]
for i, stmt in enumerate(statements, 1):
    print(f'--- Statement {i} ---')
    result = spark.sql(stmt)
    if stmt.lstrip().upper().startswith(('SELECT','SHOW')):
        result.show(truncate=False)
print('lab0: OK')
" 2>&1 | grep -E "^--- Statement|^lab0|Error|Exception" | tee -a "$OUT"

banner "STEP 2: Lab 1 Spark"
docker cp "$REPO/lab1-acid-transactions/spark/README.md" iceberg-jupyter:/tmp/lab1-spark.md
docker exec iceberg-jupyter python3 /tmp/runner.py /tmp/lab1-spark.md 2>&1 \
    | grep -E "^###|^--- block|FAILED" | tee -a "$OUT"

banner "STEP 3: Lab 1 Trino"
python3 "$RUN/trino_runner.py" "$REPO/lab1-acid-transactions/trino/README.md" 2>&1 \
    | grep -E "^###|^--- block" | tee -a "$OUT"

banner "STEP 4: Lab 1 Polaris (curl)"
python3 "$RUN/polaris_runner.py" "$REPO/lab1-acid-transactions/polaris/README.md" 2>&1 \
    | grep -E "^###|^--- block" | tee -a "$OUT"

banner "STEP 5: Lab 2 time travel (Spark)"
docker cp "$REPO/lab2-time-travel/README.md" iceberg-jupyter:/tmp/lab2.md
docker exec iceberg-jupyter python3 /tmp/runner.py /tmp/lab2.md 2>&1 \
    | grep -E "^###|^--- block|FAILED" | tee -a "$OUT"

banner "STEP 6: Lab 2 time travel (Trino)"
python3 "$RUN/trino_runner.py" "$REPO/lab2-time-travel/README.md" 2>&1 \
    | grep -E "^###|^--- block" | tee -a "$OUT"

banner "STEP 7: Lab 3 schema evolution (Spark)"
docker cp "$REPO/lab3-schema-evolution/README.md" iceberg-jupyter:/tmp/lab3.md
docker exec iceberg-jupyter python3 /tmp/runner.py /tmp/lab3.md 2>&1 \
    | grep -E "^###|^--- block|FAILED" | tee -a "$OUT"

banner "STEP 8: Lab 3 schema evolution (Trino)"
python3 "$RUN/trino_runner.py" "$REPO/lab3-schema-evolution/README.md" 2>&1 \
    | grep -E "^###|^--- block" | tee -a "$OUT"

banner "STEP 9: Bonus performance (Spark)"
docker cp "$REPO/bonus-performance/README.md" iceberg-jupyter:/tmp/bonus.md
docker exec iceberg-jupyter python3 /tmp/runner.py /tmp/bonus.md 2>&1 \
    | grep -E "^###|^--- block|FAILED" | tee -a "$OUT"

banner "STEP 10: lab0 bash blocks (startup/health checks)"
bash "$RUN/lab0_bash.sh" "$REPO/lab0-setup/README.md" 2>&1 \
    | grep -E "^###|^--- block" | tee -a "$OUT"

banner "FINAL STATE"
docker exec iceberg-trino trino --execute "SHOW TABLES FROM iceberg.tutorial" 2>&1 \
    | grep -vE "WARNING|org.jline" | tee -a "$OUT"
echo "full log: $OUT"