#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NETWORK="iot-rca-net"
IMAGE="iot-topology-rca:local"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-iot_topo_local_dev}"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

wait_for_neo4j() {
  echo "Waiting for Neo4j to become ready..."
  for i in $(seq 1 30); do
    if docker exec iot-rca-neo4j cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" "RETURN 1" >/dev/null 2>&1; then
      echo "Neo4j is ready."
      return 0
    fi
    sleep 2
  done
  echo "Neo4j did not become ready in time." >&2
  return 1
}

cmd="${1:-up}"

case "${cmd}" in
  up)
    docker network inspect "${NETWORK}" >/dev/null 2>&1 || docker network create "${NETWORK}" >/dev/null
    docker build -t "${IMAGE}" "${ROOT_DIR}"

    for c in iot-rca-simulator iot-rca-ingester iot-rca-api iot-rca-neo4j iot-rca-redis iot-rca-mosquitto; do
      docker rm -f "${c}" >/dev/null 2>&1 || true
    done

    docker run -d --name iot-rca-mosquitto --network "${NETWORK}" \
      -p 1883:1883 \
      -v "${ROOT_DIR}/mosquitto/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
      eclipse-mosquitto:2 >/dev/null

    docker run -d --name iot-rca-redis --network "${NETWORK}" \
      -p 6379:6379 \
      -v iot-rca-redis-data:/data \
      redis:7-alpine redis-server --appendonly yes >/dev/null

    docker run -d --name iot-rca-neo4j --network "${NETWORK}" \
      -p 7474:7474 -p 7687:7687 \
      -v iot-rca-neo4j-data:/data \
      -e "NEO4J_AUTH=${NEO4J_USER}/${NEO4J_PASSWORD}" \
      -e "NEO4J_server_memory_heap_initial__size=256m" \
      -e "NEO4J_server_memory_heap_max__size=512m" \
      neo4j:5-community >/dev/null

    wait_for_neo4j

    docker run -d --name iot-rca-api --network "${NETWORK}" \
      -p 8000:8000 \
      -e "REDIS_URL=redis://iot-rca-redis:6379/0" \
      -e "NEO4J_URI=bolt://iot-rca-neo4j:7687" \
      -e "NEO4J_USER=${NEO4J_USER}" \
      -e "NEO4J_PASSWORD=${NEO4J_PASSWORD}" \
      -e "MQTT_HOST=iot-rca-mosquitto" \
      -e "MQTT_PORT=1883" \
      "${IMAGE}" python -m iot_rca.api >/dev/null

    docker run -d --name iot-rca-ingester --network "${NETWORK}" \
      -e "REDIS_URL=redis://iot-rca-redis:6379/0" \
      -e "NEO4J_URI=bolt://iot-rca-neo4j:7687" \
      -e "NEO4J_USER=${NEO4J_USER}" \
      -e "NEO4J_PASSWORD=${NEO4J_PASSWORD}" \
      -e "MQTT_HOST=iot-rca-mosquitto" \
      -e "MQTT_PORT=1883" \
      -e "HEARTBEAT_TTL_SECONDS=${HEARTBEAT_TTL_SECONDS:-8}" \
      -e "EXPIRY_SWEEP_INTERVAL_SECONDS=${EXPIRY_SWEEP_INTERVAL_SECONDS:-1.0}" \
      -e "RCA_INTERVAL_SECONDS=${RCA_INTERVAL_SECONDS:-1.5}" \
      "${IMAGE}" python -m iot_rca.mqtt_ingester >/dev/null

    docker run -d --name iot-rca-simulator --network "${NETWORK}" \
      -e "MQTT_HOST=iot-rca-mosquitto" \
      -e "MQTT_PORT=1883" \
      -e "SIMULATOR_INTERVAL_SECONDS=${SIMULATOR_INTERVAL_SECONDS:-2.0}" \
      "${IMAGE}" python -m iot_rca.simulator >/dev/null

    echo "Stack is running:"
    echo "  Dashboard UI : http://localhost:8000"
    echo "  Swagger Docs : http://localhost:8000/docs"
    echo "  Neo4j Browser: http://localhost:7474"
    ;;
  down)
    for c in iot-rca-simulator iot-rca-ingester iot-rca-api iot-rca-neo4j iot-rca-redis iot-rca-mosquitto; do
      docker rm -f "${c}" >/dev/null 2>&1 || true
    done
    docker network rm "${NETWORK}" >/dev/null 2>&1 || true
    echo "Stack stopped."
    ;;
  status)
    docker ps --filter "name=iot-rca-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    ;;
  *)
    echo "Usage: $0 {up|down|status}" >&2
    exit 1
    ;;
esac
