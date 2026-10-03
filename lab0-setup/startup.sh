#!/bin/bash
# Startup script for Iceberg tutorial environment.
# Starts the stack (a no-op if it is already running), waits until every
# service answers, then prints the access points.

# docker-compose.yml lives in the project root, one level above this script.
cd "$(dirname "$0")/.." || exit 1

echo "=============================================="
echo "  Apache Iceberg Tutorial Environment Setup  "
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# How long to wait for everything to come up. The first start builds two
# images and downloads several more, so allow plenty of time.
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-300}

# Host port for JupyterLab: the environment wins, then .env, then 8888.
if [ -z "$JUPYTER_PORT" ] && [ -f .env ]; then
    JUPYTER_PORT=$(sed -n 's/^JUPYTER_PORT=//p' .env | tail -1)
fi
JUPYTER_PORT=${JUPYTER_PORT:-8888}

echo ""
echo "Checking prerequisites..."
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}ERROR: Docker is not installed or not in PATH${NC}"
    echo "Please install Docker: https://www.docker.com/get-started/"
    exit 1
fi

echo -e "${GREEN}✓ Docker is installed${NC}"

# Check if Docker Compose (v2, the `docker compose` plugin) is installed
if ! docker compose version &> /dev/null; then
    echo -e "${RED}ERROR: Docker Compose v2 is not installed${NC}"
    echo "Please install Docker Compose (included with Docker Desktop)"
    exit 1
fi

echo -e "${GREEN}✓ Docker Compose is installed${NC}"

# Another JupyterLab on the host commonly holds the Jupyter port. On macOS
# Docker does not fail in that case -- the browser just reaches the other
# server and rejects this stack's token -- so catch it up front.
if command -v lsof &> /dev/null; then
    other=$(lsof -nP -iTCP:"$JUPYTER_PORT" -sTCP:LISTEN 2>/dev/null \
        | awk 'NR > 1 && $1 !~ /^(com\.docke|docker|vpnkit)/ {print $1 " (pid " $2 ")"}' | sort -u)
    if [ -n "$other" ]; then
        echo -e "${RED}ERROR: port $JUPYTER_PORT is already used by: $other${NC}"
        echo "  That is usually another JupyterLab. Either stop it, or give this"
        echo "  tutorial a different port and run this script again:"
        echo "    cp -n .env.example .env   # if you have no .env yet"
        echo "    then set JUPYTER_PORT=8889 in .env"
        exit 1
    fi
    echo -e "${GREEN}✓ Port $JUPYTER_PORT is free for JupyterLab${NC}"
fi

echo ""
echo "Starting services..."
echo ""

if ! docker compose up -d; then
    echo -e "${RED}ERROR: docker compose up failed -- see the output above${NC}"
    exit 1
fi

# --- Health checks ------------------------------------------------------------
# Each function returns 0 once its service is ready.

# Garage is a distroless image with no shell and no health endpoint. Instead we
# check the one-shot bootstrap container, which only exits 0 after it has
# created the bucket and the access key.
check_objectstore() {
    [ "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' iceberg-objectstore-bootstrap 2>/dev/null)" = "exited 0" ]
}

# Polaris: any HTTP response at all (401 without a token is expected) means the
# server is up.
check_polaris() {
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8181/api/catalog/v1/config)
    [ -n "$code" ] && [ "$code" != "000" ]
}

# Trino reports "starting":true until it is ready to accept queries.
check_trino() {
    curl -s http://localhost:8080/v1/info | grep -q '"starting":false'
}

# JupyterLab: any HTTP response (it redirects to the login page), and the login
# token has reached the log -- that line can land a moment after the port opens,
# and the summary below needs it.
check_jupyter() {
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$JUPYTER_PORT")
    [ -n "$code" ] && [ "$code" != "000" ] || return 1
    docker logs iceberg-jupyter 2>&1 | grep -q 'token='
}

SERVICES="objectstore polaris trino jupyter"
LABEL_objectstore="Object store (Garage, port 3900)"
LABEL_polaris="Polaris (port 8181)"
LABEL_trino="Trino (port 8080)"
LABEL_jupyter="JupyterLab (port $JUPYTER_PORT)"

echo ""
echo "Waiting for services to become ready (up to ${TIMEOUT_SECONDS}s)..."

start=$(date +%s)
while true; do
    pending=""
    for s in $SERVICES; do
        "check_$s" || pending="$pending $s"
    done
    [ -z "$pending" ] && break
    if [ $(( $(date +%s) - start )) -ge "$TIMEOUT_SECONDS" ]; then
        break
    fi
    sleep 3
done

echo ""
echo "Service status:"
echo ""

all_ok=true
for s in $SERVICES; do
    label_var="LABEL_$s"
    printf '%-36s ' "${!label_var}..."
    if "check_$s"; then
        echo -e "${GREEN}✓ Ready${NC}"
    else
        echo -e "${RED}✗ Not ready${NC}"
        all_ok=false
    fi
done

if [ "$all_ok" != true ]; then
    echo ""
    echo -e "${YELLOW}Some services did not become ready. Check:${NC}"
    echo "  docker compose ps -a"
    echo "  docker compose logs <service-name>"
    echo "  (iceberg-objectstore-bootstrap and iceberg-polaris-bootstrap are"
    echo "   one-shot containers: they should show 'Exited (0)')"
fi

echo ""
echo "=============================================="
echo "  Access Points                                "
echo "=============================================="
echo ""
echo "  JupyterLab (Spark):    http://localhost:$JUPYTER_PORT"
echo "  Spark UI:              http://localhost:4040  (only while a SparkSession runs)"
echo "  Trino web UI:          http://localhost:8080  (any user name, no password)"
echo "  Polaris REST API:      http://localhost:8181/api/catalog  (OAuth2, root / root)"
echo "  Garage S3 API:         http://localhost:3900"
echo ""
echo "  There is no object-store web console and no Polaris admin UI."
echo ""
echo "  JupyterLab login URL:"
token=$(docker logs iceberg-jupyter 2>&1 | grep -o 'token=[0-9a-f]*' | tail -1)
if [ -n "$token" ]; then
    # Jupyter logs its in-container address (port 8888); print the host one.
    echo "    http://localhost:$JUPYTER_PORT/lab?$token"
else
    echo "    (not found yet) run: docker logs iceberg-jupyter 2>&1 | grep 'token=' | tail -1"
fi
echo ""
echo "  Object-store keys are generated at first boot. To see them:"
echo "    docker exec iceberg-trino cat /creds/garage-credentials.env"
echo ""
echo "  Full details: common/connection-reference.md"
echo ""
echo "=============================================="
echo "  Next Steps                                   "
echo "=============================================="
echo ""
echo "  1. Open the JupyterLab URL above in your browser"
echo "  2. Create the tutorial namespace (lab0-setup/README.md)"
echo "  3. Start with Lab 1: ACID Transactions"
echo ""

[ "$all_ok" = true ]
