#!/usr/bin/env bash
# Usage: ./run_experiment.sh <setup> <mode>
#   setup: 1dn | 3dn
#   mode:  basic | opt
set -euo pipefail

SETUP="${1:-1dn}"
MODE="${2:-basic}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_DIR/docker"

if [[ "$SETUP" == "1dn" ]]; then
    COMPOSE_FILE="$DOCKER_DIR/docker-compose-1dn.yml"
else
    COMPOSE_FILE="$DOCKER_DIR/docker-compose-3dn.yml"
fi

APP_SCRIPT="spark_app$([[ "$MODE" == "opt" ]] && echo "_opt").py"
EXPERIMENT="${MODE}_${SETUP}"

echo "experiment: $EXPERIMENT"
echo "compose: $COMPOSE_FILE"

echo "[1/5] starting hadoop ($SETUP)..."
docker compose -f "$COMPOSE_FILE" up -d namenode
sleep 10

if [[ "$SETUP" == "1dn" ]]; then
    docker compose -f "$COMPOSE_FILE" up -d datanode1
else
    docker compose -f "$COMPOSE_FILE" up -d datanode1 datanode2 datanode3
fi

echo "[2/5] waiting for HDFS..."
MAX_WAIT=120
ELAPSED=0
while true; do
    STATUS=$(docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null || echo "not ready")
    if echo "$STATUS" | grep -q "Safe mode is OFF"; then
        echo "HDFS ready"
        break
    fi
    if (( ELAPSED >= MAX_WAIT )); then
        echo "ERROR: HDFS timeout"
        docker compose -f "$COMPOSE_FILE" logs namenode | tail -30
        exit 1
    fi
    echo "$STATUS - waiting 5s... (${ELAPSED}s elapsed)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -E "^(Live|Dead)" || true

echo "[3/5] uploading dataset..."
DATA_EXISTS=$(docker exec namenode hdfs dfs -test -e /data/dataset.csv && echo "yes" || echo "no")
if [[ "$DATA_EXISTS" == "no" ]]; then
    docker exec namenode hdfs dfs -mkdir -p /data
    export MSYS_NO_PATHCONV=1
    docker exec namenode hdfs dfs -put /data/dataset.csv /data/dataset.csv
    docker exec namenode hdfs fsck /data/dataset.csv -files -blocks 2>/dev/null | grep -E "^(Status|/data|Total)" || true
else
    echo "dataset.csv already in HDFS"
fi

echo "[4/5] running spark ($APP_SCRIPT)..."
docker compose -f "$COMPOSE_FILE" build spark 2>&1 | tail -5

docker compose -f "$COMPOSE_FILE" run --rm \
    -e SETUP_NAME="$SETUP" \
    spark \
    python "/app/spark/$APP_SCRIPT" "$SETUP"

echo "[5/5] $EXPERIMENT complete"
METRICS_FILE="$PROJECT_DIR/results/metrics_${EXPERIMENT}.json"
if [[ -f "$METRICS_FILE" ]]; then
    python3 -c "
import json
with open('$METRICS_FILE') as f:
    m = json.load(f)
print(f\"total: {m['total_time_s']}s  peak_ram: {m['peak_ram_mb']}mb\")
for s in m['steps']:
    print(f\"  {s['name']:30s} {s['time_s']:6.3f}s\")
" 2>/dev/null || true
fi
