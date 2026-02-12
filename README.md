# ⬡ Pipeline Execution Service

A self-contained, observable data pipeline platform. Upload a Python script package, run it through Apache Airflow, and watch real-time metrics — task duration, exit codes, and carbon emissions — flow into Prometheus and Grafana. Everything runs locally in Docker with a single command.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Services](#services)
- [The Package Format](#the-package-format)
- [The Pipeline Runner](#the-pipeline-runner)
- [Metrics Pipeline](#metrics-pipeline)
- [API Reference](#api-reference)
- [Streamlit UI](#streamlit-ui)
- [Makefile Reference](#makefile-reference)
- [Development Guide](#development-guide)
- [Troubleshooting](#troubleshooting)

---

## Overview

Pipeline Execution Service (PES) lets you package a data transformation script, upload it through a web interface, execute it on demand via Airflow, and observe everything that happens — duration, success/failure, and estimated CO₂ emissions — in a live dashboard.

**The intended workflow:**

1. Drop a `.zip` package (DAG + script + data) onto the Streamlit UI
2. Select the DAG and click **Run Pipeline**
3. Watch the status update in real time (queued → running → success)
4. Preview and download the output CSV
5. Open the Metrics page to see Prometheus data for every run

**What makes it interesting from an engineering standpoint:**

- The runner container writes Prometheus textfile metrics directly to a shared Docker volume, which node-exporter picks up automatically — no push gateway, no custom exporter code needed beyond the runner itself
- Carbon emissions are tracked per task via [CodeCarbon](https://codecarbon.io/) and exposed as a Prometheus gauge alongside duration and exit code
- Airflow's internal StatsD metrics flow through a statsd-exporter into the same Prometheus instance, giving you scheduler health alongside task-level metrics in one place

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        User / Browser                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ http://localhost:8501
                            ▼
                   ┌─────────────────┐
                   │  Streamlit UI   │  (upload · trigger · poll · metrics)
                   └────────┬────────┘
                            │ HTTP
                            ▼
                   ┌─────────────────┐
                   │  Execution API  │  FastAPI · :8090
                   │  (FastAPI)      │  /upload  /pipelines  /files
                   └────────┬────────┘
              ┌─────────────┴──────────────┐
              │ REST API                   │ volume writes
              ▼                            ▼
   ┌──────────────────┐        ┌───────────────────┐
   │ Airflow Webserver│        │  Docker Volumes    │
   │   :8080          │        │  airflow_dags      │
   │   Scheduler      │        │  pipeline_scripts  │
   └────────┬─────────┘        │  pipeline_data     │
            │ DockerOperator   └───────────────────┘
            ▼
   ┌──────────────────┐
   │ pipeline-task-   │  spawned per task run
   │ runner:latest    │  runs transform.py
   └────────┬─────────┘
            │ writes
            ▼
   ┌──────────────────────────────────────────────────────┐
   │              node_exporter_textfile (volume)          │
   │   pipeline_sample_runner_dag_run_transform.prom       │
   └──────────────┬───────────────────────────────────────┘
                  │ reads (textfile collector)
                  ▼
   ┌──────────────────┐     scrape     ┌────────────────┐
   │  node-exporter   │ ◄────────────► │   Prometheus   │ :9090
   │  :9100           │                │                │
   └──────────────────┘                └───────┬────────┘
                                               │
   ┌──────────────────┐     scrape             │
   │  statsd-exporter │ ◄──────────────────────┤
   │  :9102 / :8125   │ ◄── Airflow StatsD     │
   └──────────────────┘                        │
                                               │
   ┌──────────────────┐     scrape             │
   │  cAdvisor        │ ◄──────────────────────┘
   │  :8081           │  container CPU/mem
   └──────────────────┘
                                    │ query
                                    ▼
                           ┌────────────────┐
                           │    Grafana     │ :3000
                           │  (dashboards)  │
                           └────────────────┘
```

**Dependency boot order:**

```
postgres (healthy)
  └── airflow-init (completed)
        ├── airflow-webserver (healthy)
        │     └── execution-api (healthy)
        │               └── streamlit (healthy)
        └── airflow-scheduler

statsd-exporter (healthy)
node-exporter (healthy)
  └── prometheus (healthy)
        └── grafana (healthy)
```

---

## Prerequisites

| Requirement | Minimum version | Notes |
|---|---|---|
| Docker Desktop | 4.x | Enable Docker socket access |
| Docker Compose | v2 (`docker compose`) | Included with Docker Desktop |
| Make | any | Pre-installed on macOS/Linux |
| Free ports | — | 3000, 8080, 8081, 8090, 8501, 9090, 9100, 9102 |
| Disk space | ~4 GB | Images + volumes |

> **macOS note:** Docker Desktop must have access to `/var/run/docker.sock`. Check in Settings → General → "Allow the default Docker socket to be used".

---

## Quick Start

### 1. Clone and enter the repo

```bash
git clone <repo-url> pipeline-execution-service
cd pipeline-execution-service
```

### 2. Build and start everything

```bash
make up
```

This single command:
- Builds the `pipeline-task-runner:latest` image
- Builds all service images (Airflow, execution-api, Streamlit)
- Starts all 10 services via Docker Compose
- Waits up to 3 minutes for every service to report healthy
- Prints a health summary and access links

Expected final output:

```
  ✓  PostgreSQL              healthy
  ✓  Airflow Webserver       healthy    http://localhost:8080/health
  ✓  Airflow Scheduler       healthy
  ✓  Execution API           healthy    http://localhost:8090/docs
  ✓  Streamlit UI            healthy    http://localhost:8501/_stcore/health
  ✓  Prometheus              healthy    http://localhost:9090/-/ready
  ✓  Grafana                 healthy    http://localhost:3000/api/health
  ✓  node-exporter           healthy    http://localhost:9100/metrics
  ✓  statsd-exporter         healthy    http://localhost:9102/metrics
  ✓  cAdvisor                healthy    http://localhost:8081/healthz

  All services healthy.

  Streamlit UI  →  http://localhost:8501
  Airflow       →  http://localhost:8080  (airflow / airflow)
  Grafana       →  http://localhost:3000  (admin / admin)
```

### 3. Upload the sample package and run

```bash
make upload     # zips and uploads packages/sample_package
```

Then open **http://localhost:8501**, select `sample_runner_dag`, and click **Run Pipeline**.

### 4. Stop everything

```bash
make down       # stop containers, keep volumes/data
make destroy    # stop containers AND delete all volumes
```

---

## Project Structure

```
pipeline-execution-service/
│
├── docker-compose.yml          # ← single entry point for the full stack
├── Makefile                    # dev/ops shortcuts
├── scaffold.sh                 # volume/network scaffold utility
│
├── airflow/
│   ├── Dockerfile              # Apache Airflow 2.8.1 + providers
│   ├── airflow.cfg
│   ├── requirements.txt        # apache-airflow-providers-docker
│   ├── dags/                   # built-in DAG stubs (not the user DAGs)
│   └── plugins/
│
├── execution_api/
│   ├── Dockerfile
│   ├── app.py                  # FastAPI routes
│   ├── schemas.py              # Pydantic models
│   ├── requirements.txt
│   ├── services/
│   │   ├── airflow_client.py   # Airflow REST API wrapper
│   │   └── package_manager.py  # zip extraction + volume writes
│   └── utils/
│       └── fs.py               # filesystem helpers
│
├── runner/
│   ├── Dockerfile              # python:3.9-slim + codecarbon
│   └── run_with_carbon.py      # entrypoint: runs scripts + emits metrics
│
├── streamlit_app/
│   ├── Home.py                 # landing page
│   ├── utils.py                # APIClient + PromClient
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── .streamlit/config.toml  # dark theme config
│   └── pages/
│       ├── 1_Run_Pipeline.py   # upload → trigger → poll → download
│       └── 2_Metrics.py        # live Prometheus dashboard
│
├── monitoring/
│   ├── prometheus.yml          # scrape config (4 targets)
│   └── grafana/
│       ├── provisioning/
│       │   ├── dashboards/dashboards.yml
│       │   └── datasources/prometheus.yml
│       └── dashboards/
│           ├── airflow.json    # Airflow StatsD metrics
│           ├── carbon.json     # emissions dashboard
│           └── containers.json # cAdvisor container metrics
│
├── packages/
│   └── sample_package/         # example pipeline package
│       ├── dags/
│       │   └── sample_runner_dag.py
│       ├── scripts/
│       │   └── transform.py
│       ├── data/
│       │   └── input.csv
│       └── README.md
│
└── scripts/
    └── healthcheck.sh          # post-startup health reporter
```

---

## Services

| Service | Port | Credentials | Purpose |
|---|---|---|---|
| **Streamlit UI** | 8501 | — | Main user interface |
| **Execution API** | 8090 | — | REST API + Swagger UI at `/docs` |
| **Airflow Webserver** | 8080 | `airflow` / `airflow` | DAG management, run history, task logs |
| **Airflow Scheduler** | — | — | Executes DAG runs |
| **PostgreSQL** | 5432 (internal) | — | Airflow metadata database |
| **Prometheus** | 9090 | — | Metrics storage and query engine |
| **Grafana** | 3000 | `admin` / `admin` | Pre-provisioned dashboards |
| **node-exporter** | 9100 | — | Host + textfile metrics |
| **statsd-exporter** | 9102 / 8125 UDP | — | Receives Airflow StatsD, exposes to Prometheus |
| **cAdvisor** | 8081 | — | Per-container CPU/memory metrics |

### Docker volumes

| Volume | Contents | Shared between |
|---|---|---|
| `airflow_dags` | User DAG Python files | execution-api, airflow-webserver, airflow-scheduler |
| `pipeline_scripts` | User script Python files | execution-api, pipeline-task-runner |
| `pipeline_data` | Input/output CSVs, emissions logs | execution-api, pipeline-task-runner |
| `node_exporter_textfile` | `.prom` metric files from runner | pipeline-task-runner, node-exporter |
| `airflow_postgres` | Airflow DB | postgres |
| `airflow_logs` | Airflow task logs | airflow-webserver, airflow-scheduler |
| `prometheus_data` | Time series data (15 day retention) | prometheus |
| `grafana_storage` | Grafana state and saved dashboards | grafana |

---

## The Package Format

A pipeline package is a `.zip` file with the following layout:

```
my_package.zip
├── dags/
│   └── my_dag.py           # Airflow DAG definition(s)
├── scripts/
│   └── my_transform.py     # Python script executed by the runner
└── data/
    └── input.csv           # Input data
```

When uploaded via `/upload` (or the Streamlit UI), each folder is extracted to the corresponding Docker volume:

| Zip folder | → | Volume | → | Mount path in runner |
|---|---|---|---|---|
| `dags/` | → | `airflow_dags` | → | `/opt/airflow/dags` |
| `scripts/` | → | `pipeline_scripts` | → | `/app/scripts` |
| `data/` | → | `pipeline_data` | → | `/app/data` |

### Writing a DAG for PES

Your DAG must use `DockerOperator` and mount the three standard volumes. Use the sample as a template:

```python
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(dag_id="my_dag", start_date=datetime(2024, 1, 1),
         schedule=None, catchup=False) as dag:

    run_task = DockerOperator(
        task_id="run_my_script",
        image="pipeline-task-runner:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "SCRIPT":       "/app/scripts/my_transform.py",
            "INPUT_FILE":   "/app/data/input.csv",
            "OUTPUT_FILE":  "/app/data/output.csv",

            # Prometheus textfile metrics
            "NODE_EXPORTER_TEXTFILE_DIR": "/node_exporter",

            # Stable labels for metric cardinality control
            "DAG_ID":  "{{ dag.dag_id }}",
            "TASK_ID": "{{ task.task_id }}",

            # Optional: enable CodeCarbon emissions tracking
            "CODECARBON_SAVE_TO_FILE": "true",
            "CODECARBON_OUTPUT_DIR":   "/app/data/emissions",
        },
        mounts=[
            Mount(source="pipeline_scripts",       target="/app/scripts",   type="volume"),
            Mount(source="pipeline_data",           target="/app/data",      type="volume"),
            Mount(source="node_exporter_textfile",  target="/node_exporter", type="volume"),
        ],
    )
```

### Writing a script for PES

Scripts are plain Python files. They are executed via `runpy.run_path()`, so they run in the same process as the runner. Use the environment variables set by the DAG:

```python
# scripts/my_transform.py
import os
import pandas as pd

input_file  = os.environ["INPUT_FILE"]
output_file = os.environ["OUTPUT_FILE"]

df = pd.read_csv(input_file)
df["value_x2"] = df["value"] * 2
df.to_csv(output_file, index=False)

print(f"Read:  {input_file} rows={len(df)}")
print(f"Wrote: {output_file}")
```

---

## The Pipeline Runner

`runner/run_with_carbon.py` is the entrypoint for the `pipeline-task-runner` image. It:

1. Reads the `SCRIPT` environment variable to find the Python script to run
2. Optionally wraps execution in a [CodeCarbon](https://codecarbon.io/) `EmissionsTracker`
3. Times the execution
4. On completion (success or failure), writes a Prometheus textfile to `NODE_EXPORTER_TEXTFILE_DIR`

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCRIPT` | ✅ | — | Absolute path to the Python script to run |
| `NODE_EXPORTER_TEXTFILE_DIR` | — | `""` (disabled) | Directory to write `.prom` metrics file |
| `DAG_ID` | — | `unknown_dag` | Label value for Prometheus metrics |
| `TASK_ID` | — | `unknown_task` | Label value for Prometheus metrics |
| `CODECARBON_SAVE_TO_FILE` | — | `false` | Set to `true` to enable emissions tracking |
| `CODECARBON_OUTPUT_DIR` | — | `/app/data/emissions` | Where CodeCarbon writes its CSV |

### Metrics written per run

The runner writes a single `.prom` file named `pipeline_{dag_id}_{task_id}.prom`:

```
# HELP pipeline_task_duration_seconds Task duration in seconds
# TYPE pipeline_task_duration_seconds gauge
pipeline_task_duration_seconds{dag_id="sample_runner_dag",task_id="run_transform"} 4.92

# HELP pipeline_task_exit_code Task exit code (0=success)
# TYPE pipeline_task_exit_code gauge
pipeline_task_exit_code{dag_id="sample_runner_dag",task_id="run_transform"} 0

# HELP pipeline_task_emissions_kg_co2 Estimated emissions for task run (kg CO2e)
# TYPE pipeline_task_emissions_kg_co2 gauge
pipeline_task_emissions_kg_co2{dag_id="sample_runner_dag",task_id="run_transform"} 1.33e-07
```

> ⚠️ **Always rebuild the runner image after editing `run_with_carbon.py`:**
> ```bash
> make build-runner
> ```
> Airflow's DockerOperator spawns a fresh container per task and will silently use a stale image if you skip this step.

---

## Metrics Pipeline

The full observability flow for custom pipeline metrics:

```
run_with_carbon.py
  │  writes pipeline_*.prom
  ▼
node_exporter_textfile (Docker volume)
  │  --collector.textfile.directory=/node_exporter
  ▼
node-exporter :9100/metrics
  │  scraped every 15s
  ▼
Prometheus :9090
  │  stored with 15-day retention
  ▼
Grafana :3000  /  Streamlit Metrics page
```

### Prometheus scrape targets

Configured in `monitoring/prometheus.yml`:

| Job | Target | Provides |
|---|---|---|
| `node-exporter` | `node-exporter:9100` | Host metrics + `pipeline_task_*` textfile metrics |
| `cadvisor` | `cadvisor:8080` | Per-container CPU, memory, network |
| `statsd-exporter` | `statsd-exporter:9102` | Airflow internal metrics (`airflow_*`) |
| `prometheus` | `prometheus:9090` | Prometheus self-metrics |

### Useful PromQL queries

```promql
# Last task duration
pipeline_task_duration_seconds{dag_id="sample_runner_dag"}

# Failed tasks (exit code != 0)
pipeline_task_exit_code != 0

# Total emissions across all tasks (kg CO2e)
sum(pipeline_task_emissions_kg_co2)

# Container memory usage (MB)
container_memory_working_set_bytes{name!=""} / 1024 / 1024

# Airflow scheduler heartbeat rate
rate(airflow_scheduler_heartbeat_total[5m])
```

---

## API Reference

Base URL: `http://localhost:8090`  
Interactive docs: `http://localhost:8090/docs`

### `POST /upload`

Upload and install a pipeline package.

**Query params:** `replace=true` (default) clears existing files before installing.

**Body:** `multipart/form-data` with field `file` containing a `.zip`.

```bash
curl -F "file=@packages/sample_package.zip" \
     "http://localhost:8090/upload?replace=true"
```

**Response:**
```json
{
  "installed": {
    "dags":    ["sample_runner_dag.py"],
    "scripts": ["transform.py"],
    "data":    ["input.csv"]
  }
}
```

---

### `GET /pipelines`

List all DAGs registered in Airflow.

```bash
curl http://localhost:8090/pipelines
```

**Response:**
```json
{
  "pipelines": [
    { "dag_id": "sample_runner_dag", "is_paused": false }
  ]
}
```

---

### `POST /pipelines/trigger`

Unpause and trigger a DAG run.

```bash
curl -X POST http://localhost:8090/pipelines/trigger \
     -H "Content-Type: application/json" \
     -d '{"dag_id": "sample_runner_dag"}'
```

**Response:**
```json
{
  "dag_id":       "sample_runner_dag",
  "run_id":       "manual__2026-02-12T20:04:58+00:00",
  "state":        "queued",
  "logical_date": "2026-02-12T20:04:58+00:00"
}
```

---

### `GET /pipelines/{dag_id}/runs/{run_id}`

Poll the status of a specific run.

```bash
curl "http://localhost:8090/pipelines/sample_runner_dag/runs/manual__2026-02-12T20%3A04%3A58%2B00%3A00"
```

**Response:**
```json
{
  "dag_id":     "sample_runner_dag",
  "run_id":     "manual__2026-02-12T20:04:58+00:00",
  "state":      "success",
  "start_date": "2026-02-12T20:04:58.393832+00:00",
  "end_date":   "2026-02-12T20:05:03.943582+00:00"
}
```

Possible `state` values: `queued`, `running`, `success`, `failed`

---

### `GET /files/{filename}`

Download a file from the data volume (e.g. `output.csv`).

```bash
curl http://localhost:8090/files/output.csv -o output.csv
```

---

## Streamlit UI

Access at **http://localhost:8501**

### Run Pipeline page

A four-step workflow on a single page:

**Step 01 — Upload package**
Drop a `.zip` file and click Upload. The installed file list (DAGs, scripts, data) is shown immediately after.

**Step 02 — Select DAG and trigger run**
The DAG dropdown auto-populates from the Execution API after a successful upload. Click **Run Pipeline** to trigger.

**Step 03 — Run status**
A status card updates every 3 seconds automatically, showing state (colour-coded), run ID, start time, and end time. Polling stops when the run reaches `success` or `failed`.

**Step 04 — Output data**
On success, `output.csv` is fetched automatically. A row-count slider lets you preview between 5 and 100 rows. A download button saves the file locally.

### Metrics page

Four tabs, auto-refreshing every 15 seconds:

**Pipeline Tasks** — one card per `(dag_id, task_id)` pair showing last duration and exit code with colour-coded status.

**Carbon Emissions** — total emissions in kg CO₂e with driving-distance and energy equivalences, plus a per-task breakdown table.

**Containers** — cAdvisor data showing CPU% and memory (MB) for every running container.

**Airflow** — raw StatsD metric dump from the statsd-exporter.

---

## Makefile Reference

```
make up              Build everything, start all services, run health check
make down            Stop all services (volumes preserved)
make destroy         Stop all services AND delete all volumes
make restart         Full restart (down + up)
make health          Run health check against running stack
make health-wait     Poll until all services healthy (max 3 min)

make build-runner    Build the pipeline-task-runner image
make verify-runner   Check runner image contains latest textfile metrics code

make upload          Zip and upload packages/sample_package
make trigger         Manually trigger sample_runner_dag via Airflow CLI
make check-dag       Verify DAG is registered and has no import errors

make logs            Tail all logs  (SERVICE=streamlit for one service)
make ps              Show container status table

make demo            Full demo: up + upload (opens with Streamlit ready to run)
make help            Show all available targets
```

---

## Development Guide

### Making changes to the runner

```bash
# 1. Edit runner/run_with_carbon.py
# 2. Rebuild the image
make build-runner

# 3. Verify the new code is in the image
make verify-runner

# 4. Trigger a test run
make trigger

# 5. Check the .prom file was written
docker run --rm -v node_exporter_textfile:/n busybox \
  cat /n/pipeline_sample_runner_dag_run_transform.prom
```

### Making changes to the Execution API or Streamlit

```bash
# Rebuild and restart only the changed service
docker compose up -d --build execution-api
docker compose up -d --build streamlit
```

### Adding a new pipeline package

1. Create a directory under `packages/` with the `dags/`, `scripts/`, `data/` structure
2. Write your DAG using `DockerOperator` (see [The Package Format](#the-package-format))
3. Write your transform script (reads `INPUT_FILE`, writes `OUTPUT_FILE`)
4. Run `make upload` (or adjust the `upload` target in the Makefile for your package name)
5. Trigger from Streamlit or `make trigger`

### Running the health check standalone

```bash
# Check once and exit
make health

# Poll until all healthy (useful in CI or after a restart)
make health-wait
```

### Viewing logs

```bash
make logs                          # tail all services
make logs SERVICE=execution-api    # tail one service
make logs SERVICE=streamlit
make logs SERVICE=airflow-scheduler
```

---

## Troubleshooting

### `make up` times out waiting for Airflow Webserver

Airflow's first-time DB migration can take 60–90 seconds. If the 3-minute timeout is hit:

```bash
# Check what airflow-init is doing
make logs SERVICE=airflow-init

# If init completed, check the webserver
make logs SERVICE=airflow-webserver
```

---

### Pipeline runs succeed but no `.prom` file appears

Work through this checklist:

```bash
# 1. Is the runner image up to date?
make verify-runner
# If it fails: make build-runner

# 2. Smoke-test the runner manually
docker run --rm \
  -v pipeline_scripts:/app/scripts \
  -v pipeline_data:/app/data \
  -v node_exporter_textfile:/node_exporter \
  -e SCRIPT=/app/scripts/transform.py \
  -e INPUT_FILE=/app/data/input.csv \
  -e OUTPUT_FILE=/app/data/output.csv \
  -e NODE_EXPORTER_TEXTFILE_DIR=/node_exporter \
  -e DAG_ID=sample_runner_dag \
  -e TASK_ID=run_transform \
  pipeline-task-runner:latest
# Must print: "Wrote textfile metrics: /node_exporter/pipeline_sample_runner_dag_run_transform.prom"

# 3. Check the volume
docker run --rm -v node_exporter_textfile:/n busybox ls -la /n
```

---

### Prometheus shows empty results for `pipeline_task_*`

```bash
# 1. Is the .prom file present?
docker run --rm -v node_exporter_textfile:/n busybox ls /n

# 2. Is node-exporter exposing it?
curl -s http://localhost:9100/metrics | grep pipeline_task_

# 3. Wait one scrape interval (15s) and query again
curl -s "http://localhost:9090/api/v1/query?query=pipeline_task_duration_seconds" \
  | python3 -m json.tool
```

---

### StatsD errors in Airflow logs

```bash
# Verify statsd-exporter is reachable from the Airflow container
docker exec compose-airflow-scheduler-1 \
  bash -lc 'nc -zv statsd-exporter 8125 || true'
# Should print: Connection ... succeeded!

# If not, restart the monitoring stack
docker compose up -d statsd-exporter
```

---

### `output.csv` not available in Streamlit after a successful run

The Streamlit UI fetches `output.csv` via `GET /files/output.csv` on the Execution API. Verify:

```bash
curl http://localhost:8090/files/output.csv
```

If this returns 404, the `pipeline_data` volume either doesn't have the file yet (the run may still be in progress) or the `GET /files/{filename}` endpoint is not present in `execution_api/app.py`. Confirm the endpoint exists and the service was rebuilt:

```bash
docker compose up -d --build execution-api
```

---

### Ports already in use

If any port is already bound on your machine:

```bash
# Find what's using a port (e.g. 8080)
lsof -i :8080

# Or on Linux
ss -tulnp | grep 8080
```

Common conflicts: another Airflow instance on 8080, another Grafana on 3000, another Prometheus on 9090.

---

### Complete reset

If you need to start completely fresh:

```bash
make destroy    # stops containers and deletes all volumes
make up         # rebuild and restart everything
make upload     # re-seed the sample package
```

---

## Credentials Summary

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | `airflow` | `airflow` |
| Grafana | http://localhost:3000 | `admin` | `admin` |
| Execution API docs | http://localhost:8090/docs | — | — |
| Prometheus | http://localhost:9090 | — | — |
| Streamlit | http://localhost:8501 | — | — |