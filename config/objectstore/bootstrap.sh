#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Idempotent Garage bootstrap.
#
# Garage (the S3-compatible object store) needs a one-time cluster layout and
# an access key before it will serve any requests. This script does all of it
# and publishes the resulting credentials to a shared volume so that Trino,
# Spark and Polaris can pick them up automatically. Students never run this by
# hand -- `docker compose up` does it for them.
# ---------------------------------------------------------------------------
set -euo pipefail

GARAGE="garage -c /etc/garage.toml"
CREDS_DIR="${CREDS_DIR:-/creds}"
CREDS_FILE="$CREDS_DIR/garage-credentials.env"
BUCKET="${S3_BUCKET:-warehouse}"
ZONE="dc"
CAPACITY="100G"
KEY_NAME="iceberg"
MAX_WAIT=90

log() { echo "[garage-bootstrap] $*"; }
die() { echo "[garage-bootstrap] ERROR: $*" >&2; exit 1; }

# --- 1. wait for the garage server to accept its own RPC port ---------------
log "waiting for garage to accept connections..."
for i in $(seq 1 "$MAX_WAIT"); do
  if $GARAGE status >/dev/null 2>&1; then
    log "garage is up after ${i}s"
    break
  fi
  [ "$i" -eq "$MAX_WAIT" ] && die "garage did not become ready in ${MAX_WAIT}s"
  sleep 1
done

# --- 2. read the node id ---------------------------------------------------
NODE_ID=$($GARAGE status 2>/dev/null | awk '/^[0-9a-f]{16}/ {print $1; exit}')
[ -n "$NODE_ID" ] || die "could not determine garage node id"
log "node id: $NODE_ID"

# --- 3. cluster layout ------------------------------------------------------
# Garage stores "which node holds how much capacity" as a versioned cluster
# layout. Until version 1 is applied it refuses every S3 request, so this has to
# happen before anything else.
#
# NB: `garage layout show --version 1` exits 0 even when no layout exists, so it
# is useless as an existence test. `garage layout show` prints
# "Current cluster layout version: N" instead, which is the real signal.
layout_version() {
  $GARAGE layout show 2>/dev/null \
    | awk -F': *' '/Current cluster layout version/ {print $2; exit}'
}

CURRENT_VERSION=$(layout_version)
log "current cluster layout version: ${CURRENT_VERSION:-unknown}"

if [ "${CURRENT_VERSION:-0}" -ge 1 ] 2>/dev/null; then
  log "cluster layout already applied"
else
  log "assigning capacity to node $NODE_ID..."
  # Not silenced: a failed assign here surfaces as a baffling "capacity (0) is
  # smaller than the replication factor" error at apply time.
  $GARAGE layout assign -z "$ZONE" -c "$CAPACITY" "$NODE_ID" \
    || die "failed to assign cluster layout"
  $GARAGE layout apply --version 1 \
    || die "failed to apply cluster layout"
  log "cluster layout applied"
fi

# The layout engine propagates asynchronously, so bucket creation can briefly
# fail right after apply. Poll for the node to actually take its role.
log "waiting for cluster layout to take effect..."
for i in $(seq 1 30); do
  if ! $GARAGE status 2>/dev/null | grep -q "NO ROLE ASSIGNED"; then
    log "layout active after ${i}s"
    break
  fi
  [ "$i" -eq 30 ] && die "cluster layout never became active"
  sleep 1
done

# --- 4. access key ---------------------------------------------------------
# `garage key create` is NOT idempotent -- it happily creates a second key with
# the same name. So we look the key up by name first and only create it when it
# is genuinely absent. (There is no `key show` subcommand; `key info` is the one
# that reads a key back, and it accepts a name as well as an ID.)
if $GARAGE key info "$KEY_NAME" >/dev/null 2>&1; then
  log "access key '$KEY_NAME' already exists"
else
  log "creating access key '$KEY_NAME'..."
  # Retry: right after the cluster layout is applied the key ring may not have
  # converged yet, and creation fails with a confusing "Layout not ready".
  for i in $(seq 1 15); do
    if $GARAGE key create "$KEY_NAME" >/dev/null 2>&1; then
      break
    fi
    [ "$i" -eq 15 ] && die "failed to create access key"
    sleep 2
  done
fi

KEY_INFO=$($GARAGE key info "$KEY_NAME" --show-secret 2>/dev/null)
ACCESS_KEY_ID=$(echo "$KEY_INFO" | awk -F: '/Key ID/ {gsub(/[[:space:]]/,"",$2); print $2}')
SECRET_ACCESS_KEY=$(echo "$KEY_INFO" | awk -F: '/Secret key/ {gsub(/[[:space:]]/,"",$2); print $2}')

[ -n "$ACCESS_KEY_ID" ] || die "could not read access key id from garage"
[ -n "$SECRET_ACCESS_KEY" ] || die "could not read secret access key from garage"
log "access key: $ACCESS_KEY_ID"

# --- 5. bucket + permissions ------------------------------------------------
if ! $GARAGE bucket list 2>/dev/null | grep -q "$BUCKET"; then
  log "creating bucket '$BUCKET'..."
  $GARAGE bucket create "$BUCKET" >/dev/null 2>&1 || die "failed to create bucket"
fi
$GARAGE bucket allow --read --write --owner "$BUCKET" --key "$ACCESS_KEY_ID" >/dev/null 2>&1 \
  || die "failed to grant bucket access"

# --- 6. publish credentials -------------------------------------------------
mkdir -p "$CREDS_DIR"
cat > "$CREDS_FILE" <<EOF
AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY
S3_BUCKET=$BUCKET
EOF
chmod 0644 "$CREDS_FILE"
log "credentials written to $CREDS_FILE"
log "bootstrap complete"