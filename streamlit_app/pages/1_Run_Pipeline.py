"""
Page 1 — Run Pipeline
Upload a .zip package → select DAG → trigger → poll status → preview + download output CSV.
"""
from __future__ import annotations

import io
import json
import time
import zipfile
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Run Pipeline · PES",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
    .stApp { background-color: #0d0f12; color: #e2e8f0; }
    section[data-testid="stSidebar"] { background-color: #111318 !important; border-right: 1px solid #1e2330; }
    h1 { font-family: 'IBM Plex Mono', monospace !important; font-size: 1.5rem !important; color: #f8fafc !important; letter-spacing: -0.02em; }
    h2 { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.85rem !important; color: #64748b !important; letter-spacing: 0.1em; text-transform: uppercase; }
    .stButton > button { background: #1d4ed8 !important; color: #fff !important; border: none !important; border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.8rem !important; letter-spacing: 0.06em; padding: 10px 24px !important; }
    .stButton > button:hover { background: #2563eb !important; }
    .stSelectbox > div > div { background: #151820 !important; border-color: #1e2330 !important; color: #e2e8f0 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.85rem !important; }
    [data-testid="stFileUploader"] { border: 1px dashed #2d3748 !important; border-radius: 4px !important; background: #111318 !important; }
    [data-testid="stDataFrame"] { border: 1px solid #1e2330 !important; border-radius: 4px !important; }
    .stAlert { border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.82rem !important; }
    #MainMenu, footer, header { visibility: hidden; }
    .badge { display:inline-block; padding:3px 10px; border-radius:2px; font-family:'IBM Plex Mono',monospace; font-size:0.72rem; font-weight:600; letter-spacing:0.08em; text-transform:uppercase; }
    .badge-success { background:#052e16; color:#4ade80; border:1px solid #166534; }
    .badge-running { background:#431407; color:#fb923c; border:1px solid #9a3412; }
    .badge-queued  { background:#1e1b4b; color:#a5b4fc; border:1px solid #3730a3; }
    .badge-failed  { background:#2d0a0a; color:#f87171; border:1px solid #991b1b; }
    .badge-unknown { background:#1a1a1a; color:#64748b; border:1px solid #334155; }
    </style>
    """,
    unsafe_allow_html=True,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils import api, cfg


# ── helpers ──────────────────────────────────────────────────────────────────
def badge(state: str) -> str:
    cls = {
        "success": "badge-success",
        "running": "badge-running",
        "queued":  "badge-queued",
        "failed":  "badge-failed",
    }.get(state, "badge-unknown")
    return f'<span class="badge {cls}">{state}</span>'


def step_header(n: str, title: str, done: bool = False) -> None:
    tick = "✓ " if done else ""
    color = "#4ade80" if done else "#64748b"
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:12px;margin:28px 0 12px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;color:{color};
                      border:1px solid {'#166534' if done else '#1e2330'};
                      background:{'#052e16' if done else '#111318'};
                      padding:3px 8px;border-radius:2px;letter-spacing:0.1em;">{n}</div>
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.9rem;color:#cbd5e1;">{tick}{title}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _build_package_zip_from_parts(
    *,
    dag_files,
    script_files,
    data_files,
    manifest_file,
    auto_dag_id: str,
    auto_linear_order: bool,
) -> tuple[bytes | None, list[str], dict | None]:
    errors: list[str] = []
    zip_buffer = io.BytesIO()
    manifest_preview = None

    dag_files = dag_files or []
    script_files = script_files or []
    data_files = data_files or []

    if manifest_file is None and not dag_files and not script_files:
        errors.append("Provide at least one DAG file, or scripts to build a generated pipeline package.")

    if manifest_file is None and not dag_files and script_files and not auto_dag_id.strip():
        errors.append("A DAG ID is required to auto-generate a pipeline manifest from scripts.")

    if errors:
        return None, errors, None

    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Preserve legacy DAG upload path if the user provided DAG files.
        for f in dag_files:
            zf.writestr(f"dags/{Path(f.name).name}", f.getvalue())

        # Scripts/data are always accepted; generated DAGs reference scripts under /scripts.
        for f in script_files:
            zf.writestr(f"scripts/{Path(f.name).name}", f.getvalue())
        for f in data_files:
            zf.writestr(f"data/{Path(f.name).name}", f.getvalue())

        if manifest_file is not None:
            manifest_name = Path(manifest_file.name).name
            if manifest_name.endswith((".yaml", ".yml", ".json")):
                zf.writestr(manifest_name, manifest_file.getvalue())
            else:
                errors.append("Manifest must be .json, .yaml, or .yml")
        elif script_files and not dag_files:
            ordered_scripts = sorted(script_files, key=lambda f: f.name.lower())
            if not auto_linear_order:
                ordered_scripts = script_files
            tasks = []
            prev_task_id = None
            for sf in ordered_scripts:
                stem = Path(sf.name).stem
                task_id = _sanitize_task_id(stem)
                task = {"id": task_id, "script": Path(sf.name).name}
                if prev_task_id:
                    task["depends_on"] = [prev_task_id]
                tasks.append(task)
                prev_task_id = task_id
            manifest_preview = {
                "dag_id": _sanitize_dag_id(auto_dag_id.strip()),
                "tags": ["ui", "generated"],
                "tasks": tasks,
            }
            zf.writestr("pipeline.json", json.dumps(manifest_preview, indent=2) + "\n")

    if errors:
        return None, errors, manifest_preview
    return zip_buffer.getvalue(), [], manifest_preview


def _sanitize_task_id(value: str) -> str:
    out = "".join(c if c.isalnum() or c in "._-" else "_" for c in value).strip("._-")
    if not out:
        out = "task"
    if not (out[0].isalpha() or out[0].isdigit() or out[0] == "_"):
        out = f"task_{out}"
    return out


def _sanitize_dag_id(value: str) -> str:
    out = "".join(c if c.isalnum() or c in "._-" else "_" for c in value).strip("._-")
    return out or "generated_pipeline"


# ── session state defaults ────────────────────────────────────────────────────
for key, default in [
    ("upload_done",     False),
    ("installed",       {}),
    ("discovered_dags", []),
    ("run_id",          None),
    ("dag_id",          None),
    ("run_state",       None),
    ("output_csv",      None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── page header ───────────────────────────────────────────────────────────────
st.markdown("# Run Pipeline")
st.markdown("## Upload · Select · Execute · Inspect")
st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — UPLOAD
# ─────────────────────────────────────────────────────────────────────────────
step_header("01", "Upload package", st.session_state.upload_done)

upload_mode = st.radio(
    "Upload Mode",
    options=["Zip package", "Build from files"],
    horizontal=True,
    label_visibility="collapsed",
)

upload_bytes = None
upload_filename = None

if upload_mode == "Zip package":
    uploaded = st.file_uploader(
        "Drop your pipeline `.zip` here",
        type=["zip"],
        label_visibility="collapsed",
        key="zip_uploader",
    )
    col_up1, _ = st.columns([2, 5])
    with col_up1:
        upload_btn = st.button("⬆  Upload Package", disabled=(uploaded is None), key="zip_upload_btn")
    if upload_btn and uploaded:
        upload_bytes = uploaded.read()
        upload_filename = uploaded.name
else:
    st.caption(
        "Upload individual files. If you do not provide `pipeline.yaml/json`, the app can create a simple "
        "linear `pipeline.json` from your scripts automatically. Users are not required to author `pipeline.yaml`."
    )
    left, right = st.columns(2)
    with left:
        dag_files = st.file_uploader(
            "DAG files (.py, optional for legacy packages)",
            type=["py"],
            accept_multiple_files=True,
            key="dag_files",
        )
        script_files = st.file_uploader(
            "Task scripts (.py)",
            type=["py"],
            accept_multiple_files=True,
            key="script_files",
        )
    with right:
        data_files = st.file_uploader(
            "Data files (csv/json/txt/parquet, optional)",
            type=["csv", "json", "txt", "parquet"],
            accept_multiple_files=True,
            key="data_files",
        )
        manifest_file = st.file_uploader(
            "Pipeline manifest (optional: pipeline.yaml/json)",
            type=["yaml", "yml", "json"],
            accept_multiple_files=False,
            key="manifest_file",
        )

    col_cfg1, col_cfg2 = st.columns([3, 2])
    with col_cfg1:
        auto_dag_id = st.text_input(
            "Auto-generated DAG ID (used only when no manifest and no DAG file is provided)",
            value="generated_pipeline",
            key="auto_dag_id",
        )
    with col_cfg2:
        auto_linear_order = st.checkbox(
            "Auto-link scripts linearly",
            value=True,
            help="If enabled, scripts are linked in filename order when auto-generating pipeline.json.",
            key="auto_linear_order",
        )

    zip_bytes, validation_errors, manifest_preview = _build_package_zip_from_parts(
        dag_files=dag_files,
        script_files=script_files,
        data_files=data_files,
        manifest_file=manifest_file,
        auto_dag_id=auto_dag_id,
        auto_linear_order=auto_linear_order,
    )

    if validation_errors:
        for err in validation_errors:
            st.warning(err)
    else:
        counts = {
            "dags": len(dag_files or []),
            "scripts": len(script_files or []),
            "data": len(data_files or []),
            "manifest": 1 if manifest_file or manifest_preview else 0,
        }
        st.caption(
            f"Ready to package: {counts['dags']} DAG file(s), {counts['scripts']} script(s), "
            f"{counts['data']} data file(s), {counts['manifest']} manifest."
        )
        if manifest_preview:
            with st.expander("Preview auto-generated pipeline.json"):
                st.code(json.dumps(manifest_preview, indent=2), language="json")

    col_up1, _ = st.columns([2, 5])
    with col_up1:
        upload_btn = st.button(
            "⬆  Build & Upload",
            disabled=(zip_bytes is None or bool(validation_errors)),
            key="build_upload_btn",
        )
    if upload_btn and zip_bytes is not None and not validation_errors:
        upload_bytes = zip_bytes
        upload_filename = "streamlit_built_package.zip"

if upload_bytes and upload_filename:
    with st.spinner("Uploading — waiting for Airflow to register DAGs (up to 60 s)…"):
        try:
            result = api.upload(upload_bytes, upload_filename)
            st.session_state.installed = result.get("installed", {})
            st.session_state.discovered_dags = result.get("discovered_dags", []) or result.get("generated_dags", [])
            st.session_state.upload_done = True
            st.session_state.run_id = None
            st.session_state.dag_id = None
            st.session_state.run_state = None
            st.session_state.output_csv = None
            st.session_state.last_upload_meta = result
        except Exception as e:
            st.error(f"Upload failed: {e}")

if st.session_state.upload_done and st.session_state.installed:
    installed = st.session_state.installed
    cols = st.columns(4)
    labels = {"dags": "DAGs", "scripts": "Scripts", "data": "Data files", "manifests": "Manifests"}
    for i, (key, label) in enumerate(labels.items()):
        files = installed.get(key, [])
        with cols[i]:
            st.markdown(
                f"""
                <div style="background:#0a1628;border:1px solid #1e3a5f;border-radius:4px;padding:14px 16px;">
                  <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;color:#3b82f6;
                              letter-spacing:0.1em;text-transform:uppercase;margin-bottom:8px;">{label}</div>
                  {''.join(f'<div style="font-family:monospace;font-size:0.78rem;color:#93c5fd;padding:2px 0;">{f}</div>' for f in files) if files else '<div style="font-size:0.78rem;color:#475569;">—</div>'}
                </div>
                """,
                unsafe_allow_html=True,
            )
    meta = st.session_state.get("last_upload_meta") or {}
    if meta:
        st.caption(
            f"Package mode: `{meta.get('package_mode', 'unknown')}` · "
            f"Generated DAGs: {', '.join(meta.get('generated_dags', [])) or '—'}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — SELECT & TRIGGER
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
step_header("02", "Select DAG and trigger run", bool(st.session_state.run_id))

# Use DAGs confirmed by Airflow at upload time.
# Fall back to a live query if the user navigated here directly (no upload this session).
dag_options = st.session_state.discovered_dags
if not dag_options:
    try:
        dag_options = [p["dag_id"] for p in api.list_pipelines()]
    except Exception as e:
        st.warning(f"Could not reach Airflow: {e}")
        dag_options = []

col_sel, col_btn = st.columns([3, 2])
with col_sel:
    selected_dag = st.selectbox(
        "DAG",
        options=dag_options if dag_options else ["— no DAGs found —"],
        disabled=(not dag_options),
        label_visibility="collapsed",
    )
with col_btn:
    trigger_btn = st.button(
        "▶  Run Pipeline",
        disabled=(not dag_options or st.session_state.run_state in ("queued", "running")),
    )

if trigger_btn and selected_dag and selected_dag != "— no DAGs found —":
    with st.spinner("Triggering…"):
        try:
            run = api.trigger(selected_dag)
            st.session_state.run_id     = run["run_id"]
            st.session_state.dag_id     = run["dag_id"]
            st.session_state.run_state  = run["state"]
            st.session_state.output_csv = None
        except Exception as e:
            st.error(f"Trigger failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — STATUS POLLING
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.run_id:
    st.divider()
    step_header("03", "Run status", st.session_state.run_state == "success")

    status_slot   = st.empty()
    progress_slot = st.empty()

    def render_status(state: str, run_id: str, start: str | None, end: str | None) -> None:
        status_slot.markdown(
            f"""
            <div style="background:#111318;border:1px solid #1e2330;border-radius:4px;
                        padding:20px 24px;display:flex;gap:40px;align-items:flex-start;">
              <div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;
                            color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px;">Status</div>
                {badge(state)}
              </div>
              <div style="flex:1;min-width:0;">
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;
                            color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px;">Run ID</div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.78rem;
                            color:#94a3b8;word-break:break-all;">{run_id}</div>
              </div>
              <div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;
                            color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px;">Started</div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.78rem;color:#94a3b8;">{start or "—"}</div>
              </div>
              <div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;
                            color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px;">Ended</div>
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.78rem;color:#94a3b8;">{end or "—"}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # initial render
    render_status(st.session_state.run_state, st.session_state.run_id, None, None)

    # live poll loop
    if st.session_state.run_state in ("queued", "running"):
        progress_slot.info("⏳  Pipeline is running — polling every 3 s…")
        for _ in range(120):   # max 6 minutes
            time.sleep(cfg.poll_interval)
            try:
                status = api.run_status(st.session_state.dag_id, st.session_state.run_id)
                st.session_state.run_state = status["state"]
                render_status(
                    status["state"],
                    status["run_id"],
                    status.get("start_date"),
                    status.get("end_date"),
                )
            except Exception:
                pass
            if st.session_state.run_state not in ("queued", "running"):
                break

        progress_slot.empty()

        if st.session_state.run_state == "success":
            progress_slot.success("✓  Pipeline completed successfully.")
        elif st.session_state.run_state == "failed":
            progress_slot.error("✗  Pipeline run failed. Check Airflow logs.")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — OUTPUT CSV
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.run_state == "success":
    st.divider()
    step_header("04", "Output data", bool(st.session_state.output_csv))

    if st.session_state.output_csv is None:
        with st.spinner("Fetching output.csv…"):
            st.session_state.output_csv = api.download_output()

    if st.session_state.output_csv:
        csv_bytes = st.session_state.output_csv
        df = pd.read_csv(io.BytesIO(csv_bytes))

        total_rows = len(df)
        col_info, col_dl = st.columns([4, 2])
        with col_info:
            st.markdown(
                f"""
                <div style="font-family:'IBM Plex Mono',monospace;font-size:0.75rem;color:#64748b;margin-bottom:12px;">
                  {total_rows} rows · {len(df.columns)} columns
                </div>
                """,
                unsafe_allow_html=True,
            )
        with col_dl:
            st.download_button(
                label="⬇  Download output.csv",
                data=csv_bytes,
                file_name="output.csv",
                mime="text/csv",
            )

        if total_rows > 1:
            n_preview = st.slider(
                "Preview rows",
                min_value=1,
                max_value=min(total_rows, 100),
                value=min(10, total_rows),
                step=1,
            )
        else:
            n_preview = total_rows
        st.dataframe(
            df.head(n_preview),
            use_container_width=True,
            height=min(40 + n_preview * 35, 500),
        )

    else:
        st.markdown(
            """
            <div style="background:#111318;border:1px solid #2d3748;border-radius:4px;
                        padding:20px 24px;font-family:'IBM Plex Mono',monospace;font-size:0.8rem;color:#64748b;">
              output.csv not available via API.<br>
              <span style="color:#475569;font-size:0.73rem;">
                Check that your pipeline script writes to <code>/app/data/output.csv</code>
                and that the <code>pipeline_data</code> volume is mounted in the execution-api container.
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

elif st.session_state.run_state == "failed":
    st.divider()
    st.error("Run failed — no output to display. Check the Airflow webserver at http://localhost:8080 for task logs.")
