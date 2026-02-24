#!/usr/bin/env bash
# =============================================================================
# Kafka-Spark Streaming Pipeline — Lifecycle Manager
# =============================================================================
# Usage:
#   ./run.sh start    — build images and start all services
#   ./run.sh stop     — stop all containers (preserve data volumes)
#   ./run.sh restart  — stop then start
#   ./run.sh reset    — stop AND delete all volumes (full wipe)
#   ./run.sh status   — show running containers and their health
#   ./run.sh logs     — tail logs from all services
#   ./run.sh logs <service>  — tail logs from a specific service
# =============================================================================

set -euo pipefail

COMPOSE="docker compose"   # Docker Compose v2 (bundled with Docker Desktop)
PROJECT="pipeline"

# Terminal colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }

# ---------------------------------------------------------------------------
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        error "Docker is not running. Please start Docker Desktop and try again."
        exit 1
    fi
}

# ---------------------------------------------------------------------------
cmd_start() {
    check_docker
    info "Building images and starting all services..."
    info "This will take a few minutes on first run (downloading base images + Kafka JARs)."

    $COMPOSE --project-name "$PROJECT" up --build -d

    echo ""
    success "All services started!"
    echo ""
    echo -e "  ${CYAN}Redpanda Console${NC}  →  http://localhost:8080   (Kafka topics & messages)"
    echo -e "  ${CYAN}Mongo Express${NC}     →  http://localhost:8081   (MongoDB browser)"
    echo -e "  ${CYAN}MinIO Console${NC}     →  http://localhost:9001   (Object storage — minioadmin / minioadmin123)"
    echo -e "  ${CYAN}Spark Master UI${NC}   →  http://localhost:8090   (Spark cluster)"
    echo -e "  ${CYAN}Spark App UI${NC}      →  http://localhost:4040   (Job stages & tasks)"
    echo -e "  ${CYAN}FastAPI Swagger${NC}   →  http://localhost:8000/docs"
    echo -e "  ${CYAN}Prometheus${NC}        →  http://localhost:9090"
    echo -e "  ${CYAN}Grafana${NC}           →  http://localhost:3000   (admin / admin)"
    echo ""
    info "Tip: run './run.sh logs' to watch all service output."
    info "Tip: run './run.sh status' to check container health."
}

# ---------------------------------------------------------------------------
cmd_stop() {
    check_docker
    info "Stopping all containers (volumes preserved)..."
    $COMPOSE --project-name "$PROJECT" down
    success "Stopped."
}

# ---------------------------------------------------------------------------
cmd_reset() {
    check_docker
    warn "This will DELETE all data volumes (Kafka, MongoDB, MinIO, Prometheus, Grafana)."
    read -rp "Are you sure? [y/N] " confirm
    if [[ "${confirm,,}" != "y" ]]; then
        info "Aborted."
        exit 0
    fi
    info "Stopping containers and removing volumes..."
    $COMPOSE --project-name "$PROJECT" down -v
    success "Full reset complete. Run './run.sh start' to restart with a clean slate."
}

# ---------------------------------------------------------------------------
cmd_restart() {
    cmd_stop
    sleep 2
    cmd_start
}

# ---------------------------------------------------------------------------
cmd_status() {
    check_docker
    info "Container status:"
    $COMPOSE --project-name "$PROJECT" ps
}

# ---------------------------------------------------------------------------
cmd_logs() {
    check_docker
    local service="${1:-}"
    if [[ -n "$service" ]]; then
        info "Tailing logs for service: $service"
        $COMPOSE --project-name "$PROJECT" logs -f "$service"
    else
        info "Tailing logs for all services (Ctrl+C to stop):"
        $COMPOSE --project-name "$PROJECT" logs -f
    fi
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
case "${1:-help}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    reset)   cmd_reset ;;
    status)  cmd_status ;;
    logs)    cmd_logs "${2:-}" ;;
    help|--help|-h)
        echo "Usage: ./run.sh {start|stop|restart|reset|status|logs [service]}"
        ;;
    *)
        error "Unknown command: $1"
        echo "Usage: ./run.sh {start|stop|restart|reset|status|logs [service]}"
        exit 1
        ;;
esac
