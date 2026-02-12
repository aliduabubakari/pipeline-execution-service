##############################################################################
#  Pipeline Execution Service — Makefile
#  Single entry point for all dev/ops tasks.
##############################################################################

COMPOSE = docker compose
RUNNER_IMAGE = pipeline-task-runner:latest
RUNNER_DOCKERFILE = runner/Dockerfile

.DEFAULT_GOAL := help

# ── Core commands ─────────────────────────────────────────────────────────────

.PHONY: up
up: build-runner   ## Build everything, start all services, then run health check
	@echo ""
	@echo "⬡  Starting full stack..."
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "⬡  Waiting for services to become healthy..."
	@bash scripts/healthcheck.sh --wait

.PHONY: down
down:   ## Stop all services (volumes preserved)
	@$(COMPOSE) down

.PHONY: destroy
destroy:   ## Stop all services AND delete all volumes (wipes data)
	@echo "⚠  This will delete all volumes and data. Press Ctrl-C to cancel."
	@sleep 3
	@$(COMPOSE) down -v

.PHONY: restart
restart: down up   ## Full restart

.PHONY: health
health:   ## Run health check against running stack
	@bash scripts/healthcheck.sh

.PHONY: health-wait
health-wait:   ## Poll until all services healthy (max 3 min)
	@bash scripts/healthcheck.sh --wait

# ── Runner image ──────────────────────────────────────────────────────────────

.PHONY: build-runner
build-runner:   ## Build the pipeline-task-runner image
	@echo "⬡  Building runner image..."
	@docker build -t $(RUNNER_IMAGE) -f $(RUNNER_DOCKERFILE) .
	@echo "✓  $(RUNNER_IMAGE) ready"

.PHONY: verify-runner
verify-runner:   ## Verify runner image contains textfile metrics code
	@echo "⬡  Verifying runner image..."
	@docker run --rm --entrypoint="" $(RUNNER_IMAGE) \
		grep -n "TEXTFILE\|emit_textfile\|node_exporter" /app/run_with_carbon.py \
		&& echo "✓  Runner image is up to date" \
		|| (echo "✗  Runner image is STALE — run: make build-runner" && exit 1)

# ── Package upload ────────────────────────────────────────────────────────────

.PHONY: upload
upload:   ## Zip and upload the sample package
	@echo "⬡  Packaging and uploading..."
	@rm -f packages/sample_package.zip
	@(cd packages/sample_package && zip -r ../sample_package.zip . -x "*.DS_Store")
	@curl -sS -F "file=@packages/sample_package.zip" \
		"http://localhost:8090/upload?replace=true" | python3 -m json.tool
	@echo "✓  Package uploaded"

# ── Logs ─────────────────────────────────────────────────────────────────────

.PHONY: logs
logs:   ## Tail logs for all services (or: make logs SERVICE=streamlit)
	@$(COMPOSE) logs -f $(SERVICE)

.PHONY: ps
ps:   ## Show status of all containers
	@$(COMPOSE) ps

# ── Trigger ───────────────────────────────────────────────────────────────────

.PHONY: trigger
trigger:   ## Manually trigger sample_runner_dag via Airflow CLI
	@docker exec -it compose-airflow-scheduler-1 \
		bash -lc 'airflow dags trigger sample_runner_dag'

.PHONY: check-dag
check-dag:   ## Check DAG is registered and has no import errors
	@docker exec compose-airflow-scheduler-1 \
		bash -lc 'airflow dags list | grep sample_runner_dag || true'
	@docker exec compose-airflow-scheduler-1 \
		bash -lc 'airflow dags list-import-errors || true'

# ── Full demo sequence ────────────────────────────────────────────────────────

.PHONY: demo
demo: up upload   ## Full demo: start stack + upload package (ready to use from Streamlit)
	@echo ""
	@echo "⬡  Demo stack ready."
	@echo "   Open Streamlit  →  http://localhost:8501"
	@echo "   Open Airflow    →  http://localhost:8080"
	@echo "   Open Grafana    →  http://localhost:3000"
	@echo ""

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help:   ## Show this help
	@echo ""
	@echo "  ⬡  Pipeline Execution Service"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""