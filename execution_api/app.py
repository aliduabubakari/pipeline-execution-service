from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, File, HTTPException, UploadFile, Query
from fastapi.responses import HTMLResponse, FileResponse
from requests.exceptions import HTTPError, RequestException

from execution_api.schemas import (
    UploadResponse,
    TriggerRequest,
    TriggerResponse,
    RunStatusResponse,
    TaskMetricsLatest,
    TaskMetricsLatestResponse,
    TaskMetricsLatestWithAirflow,
    TaskMetricsLatestWithAirflowResponse,
    MetricsCapabilitiesResponse,
    TaskMetricRangePoint,
    TaskMetricRangeSeries,
    TaskMetricsRangeResponse,
    ContainerInventoryItem,
    ContainerInventoryResponse,
)
from execution_api.services.package_manager import PackageManager
from execution_api.services.airflow_client import AirflowClient
from execution_api.services.docker_engine_client import DockerEngineClient
from execution_api.services.prometheus_client import PrometheusClient
from execution_api.utils.fs import PackageError

app = FastAPI(title="Pipeline Execution API", version="0.1.0")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DAGS_DIR     = os.environ.get("DAGS_DIR",        "/mnt/dags")
SCRIPTS_DIR  = os.environ.get("SCRIPTS_DIR",     "/mnt/scripts")
DATA_DIR     = os.environ.get("DATA_DIR",         "/mnt/data")
AIRFLOW_URL  = os.environ.get("AIRFLOW_URL",      "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER",     "airflow")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASSWORD", "airflow")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
DOCKER_API_BASE_URL = os.environ.get("DOCKER_API_BASE_URL")

pkg_mgr = PackageManager(dags_dir=DAGS_DIR, scripts_dir=SCRIPTS_DIR, data_dir=DATA_DIR)
airflow = AirflowClient(base_url=AIRFLOW_URL, username=AIRFLOW_USER, password=AIRFLOW_PASS)
prometheus = PrometheusClient(base_url=PROMETHEUS_URL)
docker_engine = DockerEngineClient(base_url=DOCKER_API_BASE_URL)


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
@app.post("/upload", response_model=UploadResponse)
async def upload_package(
    file: UploadFile = File(...),
    replace: bool = Query(True),
):
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip uploads are supported")

    try:
        try:
            before = {d["dag_id"] for d in airflow.list_dags()}
        except Exception:
            before = set()
        content = await file.read()
        result = pkg_mgr.install_zip(content, replace=replace)
    except PackageError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

    # ── Wait for Airflow to pick up every installed DAG ──────────────────
    # DAG filenames are like "sample_runner_dag.py" → dag_id is typically
    # the stem, but Airflow derives it from the file content.  We poll the
    # full DAG list and wait until at least one new DAG appears (or timeout).
    dag_files = result.installed.get("dags", [])
    discovered: list[str] = []

    if dag_files:
        # Poll for up to 60 s (Airflow rescans every 15 s by default)
        import time
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                current = airflow.list_dags()
                new_dags = [d["dag_id"] for d in current if d["dag_id"] not in before]
                if new_dags:
                    discovered = new_dags
                    break
                # Also accept if any dag matches the stem of an uploaded file
                stems = {f.removesuffix(".py") for f in dag_files}
                matched = [d["dag_id"] for d in current if d["dag_id"] in stems]
                if matched:
                    discovered = matched
                    break
            except Exception:
                pass
            time.sleep(3)

        # If we timed out, just return whatever Airflow knows now
        if not discovered:
            try:
                discovered = [d["dag_id"] for d in airflow.list_dags()]
            except Exception:
                discovered = []

    return UploadResponse(
        installed=result.installed,
        discovered_dags=discovered,
        generated_dags=result.generated_dags,
        package_mode=result.package_mode,
    )


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
@app.post("/pipelines/trigger", response_model=TriggerResponse)
async def trigger_pipeline(body: TriggerRequest):
    try:
        airflow.unpause_dag(body.dag_id)
    except HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"DAG '{body.dag_id}' not found in Airflow")
        raise HTTPException(status_code=502, detail=f"Airflow error while unpausing: {e}")

    try:
        run = airflow.trigger_dag(body.dag_id, conf=body.conf)
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Airflow error while triggering: {e}")

    return TriggerResponse(
        dag_id=run["dag_id"],
        run_id=run["dag_run_id"],
        state=run["state"],
        logical_date=run.get("logical_date") or run.get("execution_date", ""),
    )


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
@app.get("/pipelines/{dag_id}/runs/{run_id}", response_model=RunStatusResponse)
async def get_run_status(dag_id: str, run_id: str):
    try:
        run = airflow.get_dag_run(dag_id, run_id)
    except HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="DAG run not found")
        raise HTTPException(status_code=502, detail=f"Airflow error: {e}")

    return RunStatusResponse(
        dag_id=run["dag_id"],
        run_id=run["dag_run_id"],
        state=run["state"],
        start_date=run.get("start_date"),
        end_date=run.get("end_date"),
    )


# ---------------------------------------------------------------------------
# DAG list
# ---------------------------------------------------------------------------
@app.get("/pipelines")
async def list_pipelines():
    try:
        dags = airflow.list_dags()
        return {"pipelines": [{"dag_id": d["dag_id"], "is_paused": d["is_paused"]} for d in dags]}
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Airflow error: {e}")
    except RequestException as e:
        raise HTTPException(status_code=503, detail=f"Airflow unavailable: {e}")


# ---------------------------------------------------------------------------
# Task Metrics (Prometheus)
# ---------------------------------------------------------------------------
@app.get("/metrics/tasks/latest", response_model=TaskMetricsLatestResponse)
async def get_task_metrics_latest(dag_id: str | None = Query(None)):
    tasks = [TaskMetricsLatest(**row) for row in _fetch_latest_task_metric_rows(dag_id=dag_id)]
    return TaskMetricsLatestResponse(tasks=tasks)


@app.get("/metrics/tasks/latest-with-airflow", response_model=TaskMetricsLatestWithAirflowResponse)
async def get_task_metrics_latest_with_airflow(dag_id: str = Query(...)):
    metric_rows = _fetch_latest_task_metric_rows(dag_id=dag_id)

    try:
        dag_runs = airflow.list_dag_runs(dag_id, limit=1, order_by="-start_date")
    except HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found in Airflow")
        raise HTTPException(status_code=502, detail=f"Airflow error: {e}")
    except RequestException as e:
        raise HTTPException(status_code=503, detail=f"Airflow unavailable: {e}")

    latest_run = dag_runs[0] if dag_runs else None
    run_id = latest_run.get("dag_run_id") if latest_run else None
    dag_run_state = latest_run.get("state") if latest_run else None

    task_instances_by_id: dict[str, dict] = {}
    if latest_run and run_id:
        try:
            task_instances = airflow.list_task_instances(dag_id, run_id)
            task_instances_by_id = {str(ti.get("task_id")): ti for ti in task_instances if ti.get("task_id")}
        except HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Airflow error while listing task instances: {e}")
        except RequestException as e:
            raise HTTPException(status_code=503, detail=f"Airflow unavailable: {e}")

    merged_rows: list[TaskMetricsLatestWithAirflow] = []
    seen_task_ids: set[str] = set()

    for row in metric_rows:
        task_id = row["task_id"]
        seen_task_ids.add(task_id)
        ti = task_instances_by_id.get(task_id, {})
        merged_rows.append(
            TaskMetricsLatestWithAirflow(
                **row,
                run_id=run_id,
                dag_run_state=dag_run_state,
                airflow_state=ti.get("state"),
                start_date=ti.get("start_date"),
                end_date=ti.get("end_date"),
                try_number=_coerce_int(ti.get("try_number")),
            )
        )

    # Include tasks from the latest Airflow run even if Prometheus has not scraped metrics yet.
    for task_id, ti in sorted(task_instances_by_id.items(), key=lambda item: item[0]):
        if task_id in seen_task_ids:
            continue
        merged_rows.append(
            TaskMetricsLatestWithAirflow(
                dag_id=dag_id,
                task_id=task_id,
                key=f"{dag_id}/{task_id}",
                run_id=run_id,
                dag_run_state=dag_run_state,
                airflow_state=ti.get("state"),
                start_date=ti.get("start_date"),
                end_date=ti.get("end_date"),
                try_number=_coerce_int(ti.get("try_number")),
            )
        )

    merged_rows.sort(key=lambda t: (t.dag_id, t.task_id))
    return TaskMetricsLatestWithAirflowResponse(
        dag_id=dag_id,
        run_id=run_id,
        dag_run_state=dag_run_state,
        tasks=merged_rows,
    )


@app.get("/metrics/capabilities", response_model=MetricsCapabilitiesResponse)
async def get_metrics_capabilities():
    prometheus_reachable = False
    airflow_reachable = False
    docker_reachable = False

    try:
        # lightweight query to verify Prometheus reachability
        prometheus.query("up")
        prometheus_reachable = True
    except Exception:
        prometheus_reachable = False

    try:
        airflow.list_dags()
        airflow_reachable = True
    except Exception:
        airflow_reachable = False

    try:
        docker_engine.list_containers()
        docker_reachable = True
    except Exception:
        docker_reachable = False

    return MetricsCapabilitiesResponse(
        providers={
            "prometheus": {"url": PROMETHEUS_URL, "reachable": prometheus_reachable},
            "airflow": {"url": AIRFLOW_URL, "reachable": airflow_reachable},
            "docker_engine": {"url": DOCKER_API_BASE_URL or "from_env", "reachable": docker_reachable},
        },
        endpoints={
            "tasks_latest": "/metrics/tasks/latest",
            "tasks_latest_with_airflow": "/metrics/tasks/latest-with-airflow",
            "tasks_range": "/metrics/tasks/range",
        },
        task_metrics=[
            "pipeline_task_duration_seconds",
            "pipeline_task_exit_code",
            "pipeline_task_emissions_kg_co2",
        ],
        task_range_defaults={"window": "1h", "step": "30s"},
    )


@app.get("/platform/containers", response_model=ContainerInventoryResponse)
async def get_platform_containers(compose_project: str | None = Query(None)):
    try:
        rows = docker_engine.list_containers()
    except Exception as e:
        return ContainerInventoryResponse(reachable=False, containers=[], error=str(e))

    if compose_project:
        rows = [r for r in rows if r.get("compose_project") == compose_project]

    containers = [
        ContainerInventoryItem(
            id=r["id"],
            name=r.get("name"),
            status=r.get("status"),
            image=r.get("image"),
            compose_project=r.get("compose_project"),
            compose_service=r.get("compose_service"),
        )
        for r in rows
    ]
    containers.sort(key=lambda c: ((c.compose_project or ""), (c.compose_service or ""), (c.name or c.id)))
    return ContainerInventoryResponse(reachable=True, containers=containers)


@app.get("/metrics/tasks/range", response_model=TaskMetricsRangeResponse)
async def get_task_metrics_range(
    dag_id: str | None = Query(None),
    task_id: str | None = Query(None),
    metric_name: str | None = Query(
        None,
        pattern=r"^pipeline_task_(duration_seconds|exit_code|emissions_kg_co2)$",
    ),
    start: str | None = Query(None),
    end: str | None = Query(None),
    step: str = Query("30s"),
    window: str = Query("1h"),
):
    end_value = end or datetime.now(timezone.utc).isoformat()
    start_value = start or _compute_window_start_iso(end_value, window)

    metric_names = (
        [metric_name]
        if metric_name
        else [
            "pipeline_task_duration_seconds",
            "pipeline_task_exit_code",
            "pipeline_task_emissions_kg_co2",
        ]
    )

    selector = _prom_label_selector(dag_id=dag_id, task_id=task_id)
    out_series: list[TaskMetricRangeSeries] = []

    try:
        for name in metric_names:
            promql = f"{name}{selector}" if selector else name
            for raw in prometheus.query_range(promql, start=start_value, end=end_value, step=step):
                metric = raw.get("metric", {})
                d_id = metric.get("dag_id")
                t_id = metric.get("task_id")
                if not d_id or not t_id:
                    continue

                points: list[TaskMetricRangePoint] = []
                for pair in raw.get("values", []):
                    try:
                        ts = float(pair[0])
                        val = float(pair[1])
                    except Exception:
                        continue
                    points.append(TaskMetricRangePoint(ts=ts, value=val))

                out_series.append(
                    TaskMetricRangeSeries(
                        metric_name=name,
                        dag_id=str(d_id),
                        task_id=str(t_id),
                        key=f"{d_id}/{t_id}",
                        points=points,
                    )
                )
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Prometheus error: {e}")
    except RequestException as e:
        raise HTTPException(status_code=503, detail=f"Prometheus unavailable: {e}")

    out_series.sort(key=lambda s: (s.metric_name, s.dag_id, s.task_id))
    return TaskMetricsRangeResponse(
        start=start_value,
        end=end_value,
        step=step,
        filters={
            "dag_id": dag_id,
            "task_id": task_id,
            "metric_name": metric_name,
        },
        series=out_series,
    )


def _fetch_latest_task_metric_rows(*, dag_id: str | None) -> list[dict]:
    metric_queries = {
        "duration_seconds": "pipeline_task_duration_seconds",
        "exit_code": "pipeline_task_exit_code",
        "emissions_kg_co2": "pipeline_task_emissions_kg_co2",
    }

    try:
        results_by_field: dict[str, list[dict]] = {}
        for field, metric_name in metric_queries.items():
            promql = metric_name if not dag_id else f'{metric_name}{{dag_id="{dag_id}"}}'
            results_by_field[field] = prometheus.query(promql)
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Prometheus error: {e}")
    except RequestException as e:
        raise HTTPException(status_code=503, detail=f"Prometheus unavailable: {e}")

    merged: dict[tuple[str, str], dict] = {}
    for field, series_list in results_by_field.items():
        for series in series_list:
            metric = series.get("metric", {})
            d_id = metric.get("dag_id")
            t_id = metric.get("task_id")
            if not d_id or not t_id:
                continue

            key = (str(d_id), str(t_id))
            row = merged.setdefault(
                key,
                {
                    "dag_id": key[0],
                    "task_id": key[1],
                    "key": f"{key[0]}/{key[1]}",
                    "duration_seconds": None,
                    "exit_code": None,
                    "emissions_kg_co2": None,
                },
            )

            try:
                value = float(series["value"][1])
            except Exception:
                continue

            row[field] = int(value) if field == "exit_code" else value

    return [row for _, row in sorted(merged.items(), key=lambda item: (item[0][0], item[0][1]))]


def _coerce_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _prom_label_selector(*, dag_id: str | None, task_id: str | None) -> str:
    labels = []
    if dag_id:
        labels.append(f'dag_id="{dag_id}"')
    if task_id:
        labels.append(f'task_id="{task_id}"')
    return "{" + ",".join(labels) + "}" if labels else ""


def _compute_window_start_iso(end_value: str, window: str) -> str:
    end_dt = _parse_time(end_value)
    delta = _parse_window(window)
    return (end_dt - delta).isoformat()


def _parse_time(value: str) -> datetime:
    # Accept epoch seconds or ISO-8601.
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except ValueError:
        pass
    except TypeError:
        pass
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_window(value: str) -> timedelta:
    m = re.fullmatch(r"(\d+)([smhd])", value)
    if not m:
        raise HTTPException(status_code=400, detail="Invalid window. Use formats like 30m, 1h, 6h, 1d.")
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "s":
        return timedelta(seconds=n)
    if unit == "m":
        return timedelta(minutes=n)
    if unit == "h":
        return timedelta(hours=n)
    return timedelta(days=n)


# ---------------------------------------------------------------------------
# File download
# ---------------------------------------------------------------------------
@app.get("/files/{filename}")
async def download_file(filename: str):
    if not re.match(r'^[\w\-. ]+$', filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"{filename} not found in data directory")
    return FileResponse(path, media_type="text/csv", filename=filename)


# ---------------------------------------------------------------------------
# Minimal HTML UI
# ---------------------------------------------------------------------------
@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><title>Pipeline Execution</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 640px; margin: 60px auto; padding: 0 20px; }
    label { display: block; margin-top: 20px; font-weight: 600; font-size: .875rem; }
    input[type=file], select { margin-top: 6px; width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 6px; }
    button { margin-top: 20px; padding: 10px 24px; background: #2563eb; color: #fff; border: none; border-radius: 6px; cursor: pointer; }
    button:disabled { background: #93c5fd; cursor: not-allowed; }
    #status-box { margin-top: 28px; padding: 16px; border-radius: 8px; background: #f3f4f6; display: none; }
    pre { white-space: pre-wrap; word-break: break-all; margin: 0; }
  </style>
</head>
<body>
  <h1>Pipeline Execution</h1>
  <label>1. Upload package (.zip)</label>
  <input type="file" id="zip-file" accept=".zip">
  <button id="upload-btn" onclick="uploadPackage()">Upload</button>
  <div id="upload-result" style="margin-top:8px;font-size:.75rem;color:#6b7280;"></div>
  <label>2. Select DAG</label>
  <select id="dag-select"><option value="">— upload a package first —</option></select>
  <button id="run-btn" onclick="triggerRun()" disabled>Run Pipeline</button>
  <div id="status-box"><strong>Run status</strong><br><pre id="status-text"></pre></div>
<script>
  async function uploadPackage() {
    const file = document.getElementById('zip-file').files[0];
    if (!file) return alert('Select a .zip file first');
    const fd = new FormData(); fd.append('file', file);
    document.getElementById('upload-btn').disabled = true;
    document.getElementById('upload-result').textContent = 'Uploading and waiting for Airflow to register DAGs…';
    const r = await fetch('/upload?replace=true', { method: 'POST', body: fd });
    const data = await r.json();
    document.getElementById('upload-btn').disabled = false;
    if (!r.ok) { document.getElementById('upload-result').textContent = 'Error: ' + (data.detail || JSON.stringify(data)); return; }
    document.getElementById('upload-result').textContent = 'Installed: ' + JSON.stringify(data.installed);
    const sel = document.getElementById('dag-select');
    const dags = data.discovered_dags || [];
    sel.innerHTML = dags.length
      ? dags.map(id => `<option value="${id}">${id}</option>`).join('')
      : '<option value="">— no DAGs found yet —</option>';
    document.getElementById('run-btn').disabled = dags.length === 0;
  }
  async function triggerRun() {
    const dag_id = document.getElementById('dag-select').value;
    if (!dag_id) return;
    document.getElementById('run-btn').disabled = true;
    const r = await fetch('/pipelines/trigger', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ dag_id }) });
    const data = await r.json();
    if (!r.ok) { document.getElementById('run-btn').disabled = false; return alert('Trigger failed: ' + (data.detail || JSON.stringify(data))); }
    showStatus(data); pollStatus(dag_id, data.run_id);
  }
  function showStatus(data) {
    const box = document.getElementById('status-box'); box.style.display = 'block';
    document.getElementById('status-text').textContent = JSON.stringify(data, null, 2);
  }
  async function pollStatus(dag_id, run_id) {
    const r = await fetch(`/pipelines/${dag_id}/runs/${encodeURIComponent(run_id)}`);
    const data = await r.json(); showStatus(data);
    if (['queued','running'].includes(data.state)) setTimeout(() => pollStatus(dag_id, run_id), 3000);
    else document.getElementById('run-btn').disabled = false;
  }
  fetch('/pipelines').then(r=>r.json()).then(data=>{
    if (!data.pipelines?.length) return;
    const sel = document.getElementById('dag-select');
    sel.innerHTML = data.pipelines.map(p=>`<option value="${p.dag_id}">${p.dag_id}</option>`).join('');
    document.getElementById('run-btn').disabled = false;
  });
</script>
</body></html>"""
