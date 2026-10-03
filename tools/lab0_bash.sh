#!/bin/bash
# Run the bash blocks of lab0-setup against the live stack.
#
# Only the read-only verification / health-check blocks are replayed. The
# install steps (clone, startup.sh) are skipped because the stack is already up
# and rerunning startup.sh restarts containers; the interactive `trino` prompt
# and the two <placeholder> troubleshooting blocks need a human.
set -uo pipefail
README="$1"

echo "### $README"
echo "### running read-only checks (clone/up, interactive trino and <placeholder> blocks skipped)"

declare -A CHECKS=(
    [2]="docker compose ps"
    [4]="docker logs iceberg-jupyter 2>&1 | grep -c 'token=' | sed 's/^/jupyter token lines: /'"
    [5]="docker exec iceberg-trino cat /creds/garage-credentials.env | wc -l | sed 's/^/credential vars: /'"
    [6]="docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' iceberg-objectstore-bootstrap"
    [7]="docker exec iceberg-objectstore /garage bucket info warehouse | grep -E 'Size|Objects'"
    [12]="ls -l lab0-setup/spark-init.sql | sed 's/.*\///'"
    [14]="docker exec iceberg-trino trino --execute \"SELECT count(*) FROM iceberg.tutorial.orders\" 2>&1 | grep -vE 'WARNING|org.jline'"
    [15]="lsof -i :8080 >/dev/null 2>&1 && echo 'jupyter:8080 LISTENING' || echo 'jupyter:8080 NOT listening'"
)

ok=0; skipped=0; bad=0
for idx in $(printf '%s\n' "${!CHECKS[@]}" | sort -n); do
    cmd=${CHECKS[$idx]}
    out=$(eval "$cmd" 2>&1); rc=$?
    if [ $rc -ne 0 ]; then
        echo "--- block $idx: *** FAILED (rc=$rc)"
        printf '%s\n' "$out" | head -4 | sed 's/^/    /'
        bad=$((bad+1))
    else
        printf '%s\n' "$out" | head -4 | sed "s/^/--- block $idx: /"
        ok=$((ok+1))
    fi
done

for idx in 0 1 9 16 17; do
    case $idx in
        0|1|17) echo "--- block $idx: SKIP (install/restart step)";;
        9)      echo "--- block $idx: SKIP (interactive trino prompt)";;
        16)     echo "--- block $idx: SKIP (placeholder <service-name>)";;
    esac
    skipped=$((skipped+1))
done

echo
echo "### $README: $ok ok, $skipped skipped, $bad failed"
exit $((bad > 0))