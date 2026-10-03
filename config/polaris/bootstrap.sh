#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Polaris bootstrap: create the realm, then create the S3-backed catalog.
#
# The admin tool only handles realm bootstrap. Catalog creation goes through
# the management REST API with curl. Both steps are idempotent, so this is safe
# to run on every `docker compose up`.
# ---------------------------------------------------------------------------
set -euo pipefail

REALM="${POLARIS_REALM:-POLARIS}"
CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-root}"
CATALOG="${POLARIS_CATALOG:-lakehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-http://objectstore:3900}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-warehouse}"
POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"

log() { echo "[polaris-bootstrap] $*"; }

# --- 1. wait for the Polaris server -----------------------------------------
# NB: the config endpoint answers 401 when no token is supplied. That still
# proves the server is listening, so we accept any HTTP response as "up" --
# `curl -sf` would treat 401 as a failure and spin forever.
log "waiting for polaris at $POLARIS_URL ..."
for i in $(seq 1 90); do
  if curl -s -o /dev/null -w '%{http_code}' "$POLARIS_URL/api/catalog/v1/config" 2>/dev/null | grep -qE '[0-9]'; then
    log "polaris is up after ${i}s"
    break
  fi
  [ "$i" -eq 90 ] && { log "ERROR: polaris never came up"; exit 1; }
  sleep 1
done

# --- 2. bootstrap the realm -------------------------------------------------
# The admin tool exits 0 even when the realm already exists, so this is
# idempotent. The image's entrypoint is run-java.sh, which runs the jar with
# whatever args it receives -- so we call it directly.
log "bootstrapping realm '$REALM' ..."
/opt/jboss/container/java/run/run-java.sh \
  bootstrap --realm="$REALM" \
  --credential="$REALM,$CLIENT_ID,$CLIENT_SECRET" 2>&1 | grep -viE "^INFO (exec|running)" || true

# --- 3. get a token ---------------------------------------------------------
log "requesting token ..."
TOKEN=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "client_secret=$CLIENT_SECRET" \
  --data-urlencode "scope=PRINCIPAL_ROLE:ALL" | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
  log "ERROR: could not obtain a token"
  exit 1
fi
log "got token"

# --- 4. create the catalog --------------------------------------------------
# Idempotent: a 409 means it already exists.
#
# storageConfigInfo notes:
#   * pathStyleAccess must be a JSON boolean, not the string "true".
#   * There is no credentials field: Polaris uses the AWS SDK default chain,
#     i.e. the AWS_* variables its entrypoint loads from the creds volume.
#   * stsUnavailable: Garage has no STS endpoint, so Polaris cannot mint
#     short-lived, down-scoped credentials. Without this flag every table
#     operation fails trying to call STS. Engines use static keys instead.
log "creating catalog '$CATALOG' ..."
# drop-with-purge: Trino's DROP TABLE always asks Polaris to purge the files,
# which Polaris refuses (403) unless this catalog property is on.
CATALOG_PROPERTIES="{
  \"default-base-location\": \"s3://$S3_BUCKET/iceberg\",
  \"polaris.config.drop-with-purge.enabled\": \"true\"
}"

STORAGE_CONFIG="{
  \"storageType\": \"S3\",
  \"region\": \"$S3_REGION\",
  \"endpoint\": \"$S3_ENDPOINT\",
  \"pathStyleAccess\": true,
  \"stsUnavailable\": true,
  \"allowedLocations\": [\"s3://$S3_BUCKET\"]
}"

HTTP_CODE=$(curl -s -o /tmp/cat.json -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"catalog\": {
      \"name\": \"$CATALOG\",
      \"type\": \"INTERNAL\",
      \"properties\": $CATALOG_PROPERTIES,
      \"storageConfigInfo\": $STORAGE_CONFIG
    }
  }" \
  "$POLARIS_URL/api/management/v1/catalogs")

case "$HTTP_CODE" in
  201) log "catalog created" ;;
  409)
    # Already exists -- re-apply properties and storage config so a catalog created by an
    # older version of this script picks up fixes (e.g. stsUnavailable).
    log "catalog already exists; updating properties and storage config ..."
    VERSION=$(curl -sf -H "Authorization: Bearer $TOKEN" \
      "$POLARIS_URL/api/management/v1/catalogs/$CATALOG" \
      | python3 -c "import json,sys;print(json.load(sys.stdin)['entityVersion'])")
    HTTP_CODE=$(curl -s -o /tmp/cat.json -w '%{http_code}' -X PUT \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"currentEntityVersion\": $VERSION, \"properties\": $CATALOG_PROPERTIES, \"storageConfigInfo\": $STORAGE_CONFIG}" \
      "$POLARIS_URL/api/management/v1/catalogs/$CATALOG")
    [ "$HTTP_CODE" = 200 ] || { log "ERROR: catalog update returned HTTP $HTTP_CODE"; cat /tmp/cat.json; exit 1; }
    log "catalog updated"
    ;;
  *)   log "ERROR: catalog creation returned HTTP $HTTP_CODE"; cat /tmp/cat.json; exit 1 ;;
esac

# --- 5. grant data access ---------------------------------------------------
# The root principal reaches the catalog through the built-in catalog_admin
# role, which can manage the catalog but NOT its table data. Without this grant
# DROP TABLE (a purge) and metrics reporting fail with 403 "missing
# TABLE_WRITE_DATA". CATALOG_MANAGE_CONTENT covers all namespace/table/view
# operations. Granting is idempotent.
log "granting CATALOG_MANAGE_CONTENT to catalog_admin ..."
HTTP_CODE=$(curl -s -o /tmp/grant.json -w '%{http_code}' -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' \
  "$POLARIS_URL/api/management/v1/catalogs/$CATALOG/catalog-roles/catalog_admin/grants")
case "$HTTP_CODE" in
  200|201) log "grant applied" ;;
  *) log "ERROR: grant returned HTTP $HTTP_CODE"; cat /tmp/grant.json; exit 1 ;;
esac

log "bootstrap complete"
