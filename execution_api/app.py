from __future__ import annotations

import os
import re
from fastapi import FastAPI, File, HTTPException, UploadFile, Query
from fastapi.responses import HTMLResponse, FileResponse
from requests.exceptions import HTTPError, RequestException

from execution_api.schemas import (
    UploadResponse,
    TriggerRequest,
    TriggerResponse,
    RunStatusResponse,
)
from execution_api.services.package_manager import PackageManager
from execution_api.services.airflow_client import AirflowClient
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

pkg_mgr = PackageManager(dags_dir=DAGS_DIR, scripts_dir=SCRIPTS_DIR, data_dir=DATA_DIR)
airflow = AirflowClient(base_url=AIRFLOW_URL, username=AIRFLOW_USER, password=AIRFLOW_PASS)


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
        # Snapshot DAGs known BEFORE this upload so we can detect new ones
        try:
            before = {d["dag_id"] for d in airflow.list_dags()}
        except Exception:
            before = set()

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
