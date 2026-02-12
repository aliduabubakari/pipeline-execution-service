#!/usr/bin/env bash
# scripts/healthcheck.sh
# Run after `docker compose up -d` to verify every service is healthy.
# Usage:
#   ./scripts/healthcheck.sh           # check once
#   ./scripts/healthcheck.sh --wait    # poll until all healthy (max 3 min)
#
# Exit codes:
#   0 = all services healthy
#   1 = one or more services unhealthy / not running

set -euo pipefail

WAIT_MODE=false
[[ "${1:-}" == "--wait" ]] && WAIT_MODE=true

# ── colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;91m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

# ── service definitions ───────────────────────────────────────────────────────
# Format: "display_name|check_type|target"
# check_type: http | tcp | docker
SERVICES=(
  "PostgreSQL         |docker|postgres"
  "Airflow Webserver  |http  |http://localhost:8080/health"
  "Airflow Scheduler  |docker|airflow-scheduler"
  "Execution API      |http  |http://localhost:8090/docs"
  "Streamlit UI       |http  |http://localhost:8501/_stcore/health"
  "Prometheus         |http  |http://localhost:9090/-/ready"
  "Grafana            |http  |http://localhost:3000/api/health"
  "node-exporter      |http  |http://localhost:9100/metrics"
  "statsd-exporter    |http  |http://localhost:9102/metrics"
  "cAdvisor           |http  |http://localhost:8081/healthz"
)

# ── helpers ───────────────────────────────────────────────────────────────────
check_http() {
  curl -fsS --max-time 5 "$1" > /dev/null 2>&1
}

check_docker() {
  local name="$1"
  local status
  # look for container by service name (compose prefixes with project name)
  status=$(docker compose ps --format json 2>/dev/null \
    | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        obj = json.loads(line)
    except Exception:
        continue
    if '${name}' in obj.get('Service','') or '${name}' in obj.get('Name',''):
        print(obj.get('Health', obj.get('State', 'unknown')))
        break
" 2>/dev/null || echo "unknown")
  [[ "$status" == "healthy" || "$status" == "running" ]]
}

run_check() {
  local label="$1"
  local type
  local target
  type=$(echo "$2" | tr -d ' ')
  target=$(echo "$3" | tr -d ' ')

  case "$type" in
    http)   check_http "$target"   ;;
    docker) check_docker "$target" ;;
    *)      return 1 ;;
  esac
}

# ── print header ──────────────────────────────────────────────────────────────
print_header() {
  echo ""
  echo -e "${BOLD}${CYAN}  ⬡  Pipeline Execution Service — Health Report${RESET}"
  echo -e "${DIM}  $(date '+%Y-%m-%d %H:%M:%S')${RESET}"
  echo ""
  printf "  ${DIM}%-24s %-10s %s${RESET}\n" "SERVICE" "STATUS" "DETAIL"
  echo "  ──────────────────────────────────────────────────────────"
}

# ── main check loop ───────────────────────────────────────────────────────────
run_all_checks() {
  local all_ok=true
  local results=()

  for entry in "${SERVICES[@]}"; do
    IFS='|' read -r label type target <<< "$entry"
    label=$(echo "$label" | xargs)  # trim
    type=$(echo "$type"   | xargs)
    target=$(echo "$target" | xargs)

    if run_check "$label" "$type" "$target"; then
      results+=("ok|$label|$target")
    else
      results+=("fail|$label|$target")
      all_ok=false
    fi
  done

  print_header
  for result in "${results[@]}"; do
    IFS='|' read -r status label target <<< "$result"
    if [[ "$status" == "ok" ]]; then
      printf "  ${GREEN}✓${RESET}  %-24s ${GREEN}%-10s${RESET} ${DIM}%s${RESET}\n" "$label" "healthy" "$target"
    else
      printf "  ${RED}✗${RESET}  %-24s ${RED}%-10s${RESET} ${DIM}%s${RESET}\n"    "$label" "FAILED"  "$target"
    fi
  done

  echo ""

  if $all_ok; then
    echo -e "  ${GREEN}${BOLD}All services healthy.${RESET}"
    echo ""
    echo -e "  ${CYAN}Access points:${RESET}"
    echo -e "  ${DIM}Streamlit UI    →${RESET}  http://localhost:8501"
    echo -e "  ${DIM}Airflow         →${RESET}  http://localhost:8080  ${DIM}(airflow / airflow)${RESET}"
    echo -e "  ${DIM}Execution API   →${RESET}  http://localhost:8090/docs"
    echo -e "  ${DIM}Prometheus      →${RESET}  http://localhost:9090"
    echo -e "  ${DIM}Grafana         →${RESET}  http://localhost:3000  ${DIM}(admin / admin)${RESET}"
    echo ""
    return 0
  else
    echo -e "  ${RED}${BOLD}One or more services are not healthy.${RESET}"
    echo -e "  ${DIM}Run: docker compose logs <service-name>${RESET}"
    echo ""
    return 1
  fi
}

# ── wait mode: poll until all healthy or timeout ──────────────────────────────
if $WAIT_MODE; then
  MAX_WAIT=180
  INTERVAL=5
  elapsed=0

  echo ""
  echo -e "${BOLD}${CYAN}  ⬡  Waiting for all services to become healthy...${RESET}"
  echo -e "${DIM}  (timeout: ${MAX_WAIT}s)${RESET}"

  while true; do
    # suppress output while polling
    all_ok=true
    for entry in "${SERVICES[@]}"; do
      IFS='|' read -r label type target <<< "$entry"
      type=$(echo "$type" | xargs)
      target=$(echo "$target" | xargs)
      if ! run_check "$label" "$type" "$target" 2>/dev/null; then
        all_ok=false
        break
      fi
    done

    if $all_ok; then
      run_all_checks
      exit 0
    fi

    if [[ $elapsed -ge $MAX_WAIT ]]; then
      echo -e "  ${YELLOW}Timeout reached. Running final check:${RESET}"
      run_all_checks
      exit $?
    fi

    printf "  ${DIM}%ds elapsed — retrying in %ds...${RESET}\r" "$elapsed" "$INTERVAL"
    sleep $INTERVAL
    elapsed=$((elapsed + INTERVAL))
  done
else
  run_all_checks
  exit $?
fi