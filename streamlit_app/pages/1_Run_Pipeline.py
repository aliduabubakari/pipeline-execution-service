"""
Page 1 — Run Pipeline
Upload a .zip package → select DAG → trigger → poll status → preview + download output CSV.
"""
from __future__ import annotations

import io
import time
import pandas as pd
import streamlit as st

# shared style is applied in Home.py; re-apply page config here for direct navigation
st.set_page_config(
    page_title="Run Pipeline · PES",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# inject styles (needed when landing directly on this page)
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


# ── helpers ─────────────────────────────────────────────────────────────────
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


# ── state ────────────────────────────────────────────────────────────────────
if "upload_done"  not in st.session_state: st.session_state.upload_done  = False
if "installed"    not in st.session_state: st.session_state.installed    = {}
if "run_id"       not in st.session_state: st.session_state.run_id       = None
if "dag_id"       not in st.session_state: st.session_state.dag_id       = None
if "run_state"    not in st.session_state: st.session_state.run_state    = None
if "output_csv"   not in st.session_state: st.session_state.output_csv   = None


# ── page header ──────────────────────────────────────────────────────────────
st.markdown("# Run Pipeline")
st.markdown("## Upload · Select · Execute · Inspect")
st.divider()


# ────────────────────────────────────────────────────────────────────────────
# STEP 1 — UPLOAD
# ────────────────────────────────────────────────────────────────────────────
step_header("01", "Upload package", st.session_state.upload_done)

uploaded = st.file_uploader(
    "Drop your pipeline `.zip` here",
    type=["zip"],
    label_visibility="collapsed",
)

col_up1, col_up2 = st.columns([2, 5])
with col_up1:
    upload_btn = st.button("⬆  Upload Package", disabled=(uploaded is None))

if upload_btn and uploaded:
    with st.spinner("Uploading and installing…"):
        try:
            result = api.upload(uploaded.read(), uploaded.name)
            st.session_state.installed   = result.get("installed", {})
            st.session_state.upload_done = True
            st.session_state.run_id      = None
            st.session_state.run_state   = None
            st.session_state.output_csv  = None
        except Exception as e:
            st.error(f"Upload failed: {e}")

if st.session_state.upload_done and st.session_state.installed:
    installed = st.session_state.installed
    cols = st.columns(3)
    labels = {"dags": "DAGs", "scripts": "Scripts", "data": "Data files"}
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


# ────────────────────────────────────────────────────────────────────────────
# STEP 2 — SELECT & TRIGGER
# ────────────────────────────────────────────────────────────────────────────
st.divider()
step_header("02", "Select DAG and trigger run", bool(st.session_state.run_id))

pipelines = []
if st.session_state.upload_done:
    try:
        pipelines = api.list_pipelines()
    except Exception as e:
        st.warning(f"Could not reach Airflow: {e}")

dag_options = [p["dag_id"] for p in pipelines]

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
            st.session_state.run_id    = run["run_id"]
            st.session_state.dag_id    = run["dag_id"]
            st.session_state.run_state = run["state"]
            st.session_state.output_csv = None
        except Exception as e:
            st.error(f"Trigger failed: {e}")


# ────────────────────────────────────────────────────────────────────────────
# STEP 3 — STATUS POLLING
# ────────────────────────────────────────────────────────────────────────────
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
    render_status(
        st.session_state.run_state,
        st.session_state.run_id,
        None, None,
    )

    # live poll loop
    if st.session_state.run_state in ("queued", "running"):
        progress_slot.info("⏳  Pipeline is running — polling every 3 s…")
        for _ in range(120):   # max 6 minutes of polling
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


# ────────────────────────────────────────────────────────────────────────────
# STEP 4 — OUTPUT CSV
# ────────────────────────────────────────────────────────────────────────────
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

        # preview with row slider
        n_preview = st.slider(
            "Preview rows",
            min_value=5,
            max_value=min(total_rows, 100),
            value=min(10, total_rows),
            step=5,
        )
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
                Add a <code>GET /files/output.csv</code> endpoint to execution_api, or mount
                the <code>pipeline_data</code> volume directly into the Streamlit container.
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

elif st.session_state.run_state == "failed":
    st.divider()
    st.error("Run failed — no output to display. Check the Airflow webserver at http://localhost:8080 for task logs.")