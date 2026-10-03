#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Trino entrypoint: render the Iceberg catalog file, then start the server.
#
# Why this exists: the object store generates a fresh access key the first time
# it boots, so the catalog file cannot be a static file in the repo. The
# bootstrap container publishes the credentials to a shared volume and we
# substitute them here. `envsubst` is not available in the Trino image, so this
# does the substitution with plain shell.
# ---------------------------------------------------------------------------
set -euo pipefail

CREDS_FILE="${CREDS_FILE:-/creds/garage-credentials.env}"
TEMPLATE="${TEMPLATE:-/opt/trino-config/iceberg.properties.template}"
TARGET="${TARGET:-/etc/trino/catalog/iceberg.properties}"

# Block until the object store has published its credentials. Without this the
# first `docker compose up` races the bootstrap container and Trino dies.
if [ ! -f "$CREDS_FILE" ]; then
  echo "[trino-entrypoint] waiting for object store credentials at $CREDS_FILE ..."
  for i in $(seq 1 120); do
    [ -f "$CREDS_FILE" ] && break
    sleep 1
  done
  [ -f "$CREDS_FILE" ] || { echo "[trino-entrypoint] ERROR: credentials never appeared" >&2; exit 1; }
fi

echo "[trino-entrypoint] reading credentials from $CREDS_FILE"
# shellcheck disable=SC1090
set -a; . "$CREDS_FILE"; set +a

: "${AWS_ACCESS_KEY_ID:?missing AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?missing AWS_SECRET_ACCESS_KEY}"
: "${S3_BUCKET:?missing S3_BUCKET}"

export S3_ENDPOINT="${S3_ENDPOINT:-http://objectstore:3900}"
export S3_REGION="${S3_REGION:-us-east-1}"
export POLARIS_URI="${POLARIS_URI:-http://polaris:8181/api/catalog}"
export POLARIS_TOKEN_URI="${POLARIS_TOKEN_URI:-http://polaris:8181/api/catalog/v1/oauth/tokens}"
export POLARIS_CREDENTIAL="${POLARIS_CREDENTIAL:-root:root}"
export POLARIS_REALM="${POLARIS_REALM:-POLARIS}"

echo "[trino-entrypoint] rendering $TARGET"
mkdir -p "$(dirname "$TARGET")"

# Substitute only the placeholders we know about, leaving the rest of the file
# (comments, blank lines) untouched.
while IFS= read -r line; do
  out="$line"
  for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_BUCKET \
            S3_ENDPOINT S3_REGION POLARIS_URI POLARIS_TOKEN_URI POLARIS_CREDENTIAL \
            POLARIS_CATALOG POLARIS_REALM; do
    out="${out//\$\{${var}\}/${!var}}"
  done
  printf '%s\n' "$out"
done < "$TEMPLATE" > "$TARGET"

echo "[trino-entrypoint] starting trino"
exec /usr/lib/trino/bin/run-trino "$@"
