#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Lab2 - Hadoop + Spark - All Experiments"
echo "project: $PROJECT_DIR"

for cmd in docker python3; do
    command -v "$cmd" &>/dev/null || { echo "ERROR: '$cmd' not found"; exit 1; }
done
docker info &>/dev/null || { echo "ERROR: Docker is not running"; exit 1; }

DATASET="$PROJECT_DIR/data/dataset.csv"
if [[ ! -f "$DATASET" ]]; then
    echo "generating dataset..."
    python3 "$PROJECT_DIR/data/generate_dataset.py"
else
    echo "dataset exists: $DATASET"
fi

mkdir -p "$PROJECT_DIR/results"

run_on_cluster() {
    local COMPOSE_FILE="$1"
    local SETUP="$2"

    echo ""
    echo "cluster: $SETUP"

    echo "starting namenode..."
    docker compose -f "$COMPOSE_FILE" up -d namenode
    sleep 10

    if [[ "$SETUP" == "1dn" ]]; then
        docker compose -f "$COMPOSE_FILE" up -d datanode1
    else
        docker compose -f "$COMPOSE_FILE" up -d datanode1 datanode2 datanode3
    fi

    echo "waiting for HDFS..."
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
            docker compose -f "$COMPOSE_FILE" down -v
            exit 1
        fi
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done

    docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -E "^(Live datanodes|Dead datanodes)" || true

    DATA_EXISTS=$(docker exec namenode hdfs dfs -test -e /data/dataset.csv && echo "yes" || echo "no")
    if [[ "$DATA_EXISTS" == "no" ]]; then
        echo "uploading dataset to HDFS..."
        docker exec namenode hdfs dfs -mkdir -p /data
        export MSYS_NO_PATHCONV=1
        docker exec namenode hdfs dfs -put /data/dataset.csv /data/dataset.csv
        docker exec namenode hdfs fsck /data/dataset.csv -files -blocks 2>/dev/null \
            | grep -E "(Status|Total blocks|dataset)" || true
    fi

    echo "building spark image..."
    docker compose -f "$COMPOSE_FILE" build spark 2>&1 | tail -3

    echo "running: basic_$SETUP"
    START_TS=$(date +%s)
    docker compose -f "$COMPOSE_FILE" run --rm \
        -e SETUP_NAME="$SETUP" \
        spark \
        python "/app/spark/spark_app.py" "$SETUP"
    echo "basic_$SETUP done in $(( $(date +%s) - START_TS ))s"

    echo "running: opt_$SETUP"
    START_TS=$(date +%s)
    docker compose -f "$COMPOSE_FILE" run --rm \
        -e SETUP_NAME="$SETUP" \
        spark \
        python "/app/spark/spark_app_opt.py" "$SETUP"
    echo "opt_$SETUP done in $(( $(date +%s) - START_TS ))s"

    echo "stopping cluster $SETUP..."
    docker compose -f "$COMPOSE_FILE" down
}

run_on_cluster "$PROJECT_DIR/docker/docker-compose-1dn.yml" "1dn"
run_on_cluster "$PROJECT_DIR/docker/docker-compose-3dn.yml" "3dn"

echo ""
echo "generating comparison plots..."
pip install matplotlib seaborn numpy -q 2>/dev/null || true
python3 "$PROJECT_DIR/analysis/compare_results.py"

echo ""
echo "all experiments complete, results in lab2/results/"
