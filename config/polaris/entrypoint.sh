#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Polaris entrypoint: load the object-store credentials, then start the server.
#
# Polaris writes table metadata to the object store itself, so it needs the
# access key that objectstore-bootstrap generates on first boot. That key is not
# known in advance, so it cannot live in .env -- it is read from the shared
# credentials volume here instead.
# ---------------------------------------------------------------------------
set -euo pipefail

CREDS_FILE="${CREDS_FILE:-/creds/garage-credentials.env}"

for i in $(seq 1 120); do
  [ -f "$CREDS_FILE" ] && break
  sleep 1
done
[ -f "$CREDS_FILE" ] || { echo "[polaris-entrypoint] ERROR: $CREDS_FILE never appeared" >&2; exit 1; }

echo "[polaris-entrypoint] reading credentials from $CREDS_FILE"
# shellcheck disable=SC1090
set -a; . "$CREDS_FILE"; set +a

exec /opt/jboss/container/java/run/run-java.sh "$@"
