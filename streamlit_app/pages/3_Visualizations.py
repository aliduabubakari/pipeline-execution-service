"""
Page 3 — Visualizations
Task-specific visualizations for latest runs and live task containers.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Visualizations · PES",
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
    h1 { font-family: 'IBM Plex Mono', monospace !important; color: #f8fafc !important; }
    h2 { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.85rem !important; color: #64748b !important; letter-spacing: 0.1em; text-transform: uppercase; }
    [data-testid="stDataFrame"] { border: 1px solid #1e2330 !important; border-radius: 4px !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 2px; border-bottom: 1px solid #1e2330; }
    .stTabs [data-baseweb="tab"] { background: transparent !important; color: #64748b !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.73rem !important; letter-spacing: 0.06em; text-transform: uppercase; padding: 8px 18px !important; border-radius: 0 !important; }
    .stTabs [aria-selected="true"] { color: #f0f6ff !important; border-bottom: 2px solid #3b82f6 !important; }
    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils import api, prom


def section(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div style="margin: 22px 0 10px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:#3b82f6;
                      letter-spacing:0.12em;text-transform:uppercase;">{title}</div>
          {f'<div style="font-size:0.82rem;color:#64748b;margin-top:4px;">{subtitle}</div>' if subtitle else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


def no_data_card(msg: str) -> None:
    st.markdown(
        f"""
        <div style="background:#111318;border:1px dashed #1e2330;border-radius:4px;
                    padding:16px 18px;color:#64748b;font-family:'IBM Plex Mono',monospace;font-size:0.78rem;">
          {msg}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _safe_name(value: str) -> str:
    out = "".join(c if c.isalnum() or c in "._-" else "_" for c in value).strip("._-")
    return out or "x"


def _task_container_prefix(dag_id: str, task_id: str) -> str:
    return f"pes_{_safe_name(dag_id)}_{_safe_name(task_id)}_"


def _to_dt(value: str | None):
    if not value:
        return None
    try:
        v = value[:-1] + "+00:00" if value.endswith("Z") else value
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _range_df(range_payload: dict) -> pd.DataFrame:
    rows = []
    for s in range_payload.get("series", []):
        for p in s.get("points", []):
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(float(p["ts"]), tz=timezone.utc),
                    "metric_name": s.get("metric_name"),
                    "task_id": s.get("task_id"),
                    "dag_id": s.get("dag_id"),
                    "value": float(p["value"]),
                }
            )
    return pd.DataFrame(rows)


def _container_id_from_metric(metric: dict) -> str | None:
    cid = metric.get("id")
    if isinstance(cid, str) and cid.startswith("/docker/"):
        return cid.split("/")[-1]
    return None


if "viz_last_refresh" not in st.session_state:
    st.session_state.viz_last_refresh = time.time()

left, right = st.columns([6, 2])
with left:
    st.markdown("# Visualizations")
    st.markdown("## Task-specific performance and live per-task container resource charts")
with right:
    manual_refresh = st.button("↻  Refresh now")

st.divider()

try:
    pipelines = api.list_pipelines()
    dag_ids = [p["dag_id"] for p in pipelines]
except Exception as e:
    dag_ids = []
    st.warning(f"Could not load DAG list: {e}")

with st.sidebar:
    st.markdown("### Visualization Filters")
    selected_dag = st.selectbox("DAG", dag_ids if dag_ids else ["— no DAGs —"])
    window = st.selectbox("Historical window", ["15m", "1h", "6h", "24h"], index=1)
    step = st.selectbox("Prometheus step", ["15s", "30s", "60s"], index=1)
    compose_project = st.text_input("Compose project", value="pipeline-execution-service")
    auto_refresh = st.checkbox("Auto-refresh (10s)", value=True)

selected_dag_valid = bool(dag_ids) and selected_dag != "— no DAGs —"

capabilities = None
try:
    capabilities = api.metrics_capabilities()
except Exception:
    capabilities = None

top1, top2, top3 = st.columns(3)
if capabilities:
    providers = capabilities.get("providers", {})
    top1.metric("Prometheus", "up" if providers.get("prometheus", {}).get("reachable") else "down")
    top2.metric("Airflow", "up" if providers.get("airflow", {}).get("reachable") else "down")
    top3.metric("Docker Engine", "up" if providers.get("docker_engine", {}).get("reachable") else "down")
else:
    top1.metric("Prometheus", "—")
    top2.metric("Airflow", "—")
    top3.metric("Docker Engine", "—")

tab_perf, tab_container, tab_compare = st.tabs(["Task Performance", "Per-Task Containers", "Correlation"])

latest = None
latest_tasks = []
if selected_dag_valid:
    try:
        latest = api.task_metrics_latest_with_airflow(selected_dag)
        latest_tasks = latest.get("tasks", [])
    except Exception as e:
        st.error(f"Could not load latest task metrics for DAG `{selected_dag}`: {e}")

with tab_perf:
    section("Latest Task Summary", "Execution API merged latest run + Prometheus task metrics")
    if not selected_dag_valid:
        no_data_card("Select a DAG to visualize.")
    elif not latest_tasks:
        no_data_card("No task metrics found yet for this DAG.")
    else:
        summary_df = pd.DataFrame(
            [
                {
                    "task_id": t.get("task_id"),
                    "airflow_state": t.get("airflow_state"),
                    "duration_seconds": t.get("duration_seconds"),
                    "exit_code": t.get("exit_code"),
                    "emissions_kg_co2": t.get("emissions_kg_co2"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date"),
                }
                for t in latest_tasks
            ]
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Run State", latest.get("dag_run_state", "—"))
        c2.metric("Tasks", len(summary_df))
        c3.metric("Total Duration (sum)", f"{summary_df['duration_seconds'].fillna(0).sum():.2f}s")
        c4.metric("Total Emissions", f"{summary_df['emissions_kg_co2'].fillna(0).sum():.3e} kg")

        left_chart, right_chart = st.columns(2)
        with left_chart:
            section("Duration by Task")
            st.bar_chart(summary_df.set_index("task_id")[["duration_seconds"]], use_container_width=True)
        with right_chart:
            section("Emissions by Task")
            st.bar_chart(summary_df.set_index("task_id")[["emissions_kg_co2"]], use_container_width=True)

        section("Latest Run Timeline (table)")
        timeline_rows = []
        for t in latest_tasks:
            start_dt = _to_dt(t.get("start_date"))
            end_dt = _to_dt(t.get("end_date"))
            wall_s = (end_dt - start_dt).total_seconds() if (start_dt and end_dt) else None
            timeline_rows.append(
                {
                    "task_id": t.get("task_id"),
                    "airflow_state": t.get("airflow_state"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date"),
                    "wall_time_seconds": wall_s,
                    "metric_duration_seconds": t.get("duration_seconds"),
                }
            )
        st.dataframe(pd.DataFrame(timeline_rows), use_container_width=True, hide_index=True)

        section("Historical Task Trends", "Task-specific Prometheus ranges through execution-api")
        try:
            range_payload = api.task_metrics_range(dag_id=selected_dag, window=window, step=step)
            df_range = _range_df(range_payload)
        except Exception as e:
            df_range = pd.DataFrame()
            st.error(f"Could not load task range metrics: {e}")

        if df_range.empty:
            no_data_card("No range series available for the selected window.")
        else:
            metric_options = sorted(df_range["metric_name"].dropna().unique().tolist())
            selected_metric = st.selectbox("Metric series", metric_options, key="perf_metric_choice")
            df_m = df_range[df_range["metric_name"] == selected_metric].copy()
            df_m["series"] = df_m["task_id"]
            pivot = df_m.pivot_table(index="timestamp", columns="series", values="value", aggfunc="last").sort_index()
            st.line_chart(pivot, height=320, use_container_width=True)
            with st.expander("Raw series payload"):
                st.json(range_payload)

with tab_container:
    section("Per-Task Container Resource Consumption (Live)", "Maps running Docker task containers to cAdvisor metrics")
    if not selected_dag_valid:
        no_data_card("Select a DAG to visualize.")
    elif not latest_tasks:
        no_data_card("No latest task metadata available yet for this DAG.")
    else:
        try:
            inv = api.platform_containers(compose_project=compose_project or None)
        except Exception as e:
            inv = {"reachable": False, "containers": [], "error": str(e)}

        if not inv.get("reachable"):
            no_data_card(
                "Docker container inventory is unavailable from execution-api. "
                f"Error: {inv.get('error', 'unknown')}"
            )
        else:
            containers = inv.get("containers", [])
            matched_rows = []
            task_to_container = {}
            for t in latest_tasks:
                task_id = str(t.get("task_id"))
                prefix = _task_container_prefix(selected_dag, task_id)
                for c in containers:
                    name = str(c.get("name") or "")
                    if name.startswith(prefix):
                        cid = str(c.get("id") or "")
                        task_to_container[task_id] = c
                        matched_rows.append(
                            {
                                "task_id": task_id,
                                "container_name": name,
                                "container_id": cid[:12],
                                "status": c.get("status"),
                                "compose_service": c.get("compose_service"),
                            }
                        )
                        break

            if not matched_rows:
                no_data_card(
                    "No running task containers were found for the selected DAG. "
                    "Task containers are short-lived (`auto_remove=True`), so per-container cAdvisor charts are most useful during active runs. "
                    "Task-level historical metrics remain available in the Task Performance tab."
                )
            else:
                st.dataframe(pd.DataFrame(matched_rows), use_container_width=True, hide_index=True)

                metric_kind = st.selectbox(
                    "Container metric",
                    ["CPU % (live)", "Memory MB (live)"],
                    index=0,
                    key="container_metric_kind",
                )
                promql_metric = (
                    "rate(container_cpu_usage_seconds_total{id!=\"/\",cpu=\"total\"}[1m])"
                    if metric_kind.startswith("CPU")
                    else "container_memory_working_set_bytes{id!=\"/\"}"
                )

                try:
                    raw = prom.query(promql_metric)
                except Exception:
                    raw = []

                live_vals = {}
                for r in raw:
                    cid = _container_id_from_metric(r.get("metric", {}))
                    if not cid:
                        continue
                    try:
                        v = float(r["value"][1])
                    except Exception:
                        continue
                    if metric_kind.startswith("CPU"):
                        v = v * 100.0
                    else:
                        v = v / (1024 ** 2)
                    live_vals[cid] = v

                bar_rows = []
                for task_id, c in task_to_container.items():
                    cid = str(c.get("id") or "")
                    bar_rows.append(
                        {
                            "task_id": task_id,
                            "value": live_vals.get(cid),
                            "container_name": c.get("name"),
                        }
                    )
                df_bar = pd.DataFrame(bar_rows).set_index("task_id")
                if "value" in df_bar.columns:
                    st.bar_chart(df_bar[["value"]], use_container_width=True)

                section("Per-task container trend (while container is still running)")
                selected_task_for_container = st.selectbox(
                    "Task container",
                    list(task_to_container.keys()),
                    key="task_container_pick",
                )
                c = task_to_container[selected_task_for_container]
                cid = str(c.get("id"))
                cid_label = f"/docker/{cid}"
                q_cpu = f'rate(container_cpu_usage_seconds_total{{id="{cid_label}",cpu="total"}}[1m])'
                q_mem = f'container_memory_working_set_bytes{{id="{cid_label}"}}'
                end_ts = datetime.now(timezone.utc).isoformat()
                start_ts = (datetime.now(timezone.utc) - pd.Timedelta(window)).isoformat()
                cpu_series = prom.query_range(q_cpu, start=start_ts, end=end_ts, step=step)
                mem_series = prom.query_range(q_mem, start=start_ts, end=end_ts, step=step)

                trend_rows = []
                for r in cpu_series:
                    for ts, v in r.get("values", []):
                        try:
                            trend_rows.append(
                                {
                                    "timestamp": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                                    "series": "cpu_pct",
                                    "value": float(v) * 100.0,
                                }
                            )
                        except Exception:
                            continue
                for r in mem_series:
                    for ts, v in r.get("values", []):
                        try:
                            trend_rows.append(
                                {
                                    "timestamp": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                                    "series": "mem_mb",
                                    "value": float(v) / (1024 ** 2),
                                }
                            )
                        except Exception:
                            continue

                if not trend_rows:
                    no_data_card("No container trend points available yet for this running task container.")
                else:
                    df_trend = pd.DataFrame(trend_rows)
                    for label in ["cpu_pct", "mem_mb"]:
                        df_one = df_trend[df_trend["series"] == label]
                        if df_one.empty:
                            continue
                        st.markdown(f"**{label}**")
                        pivot = df_one.pivot_table(index="timestamp", values="value", aggfunc="last").sort_index()
                        st.line_chart(pivot, use_container_width=True, height=220)

with tab_compare:
    section("Execution API vs Task Metrics Consistency", "Task-specific metrics from execution-api should align with Prometheus-backed latest values")
    if not selected_dag_valid:
        no_data_card("Select a DAG to visualize.")
    elif not latest_tasks:
        no_data_card("No latest task metrics available.")
    else:
        try:
            latest_plain = api.task_metrics_latest(selected_dag)
            plain_by_task = {t["task_id"]: t for t in latest_plain.get("tasks", [])}
        except Exception as e:
            plain_by_task = {}
            st.error(f"Could not load /metrics/tasks/latest for comparison: {e}")

        rows = []
        for t in latest_tasks:
            task_id = t.get("task_id")
            p = plain_by_task.get(task_id, {})
            rows.append(
                {
                    "task_id": task_id,
                    "duration_latest_with_airflow": t.get("duration_seconds"),
                    "duration_latest": p.get("duration_seconds"),
                    "exit_latest_with_airflow": t.get("exit_code"),
                    "exit_latest": p.get("exit_code"),
                    "emissions_latest_with_airflow": t.get("emissions_kg_co2"),
                    "emissions_latest": p.get("emissions_kg_co2"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.divider()
elapsed = int(time.time() - st.session_state.viz_last_refresh)
remaining = max(0, 10 - elapsed)
st.caption(
    "Task visualizations are task-specific by design (Execution API + Prometheus). "
    f"Next refresh in {remaining}s."
)

if manual_refresh or (auto_refresh and elapsed >= 10):
    st.session_state.viz_last_refresh = time.time()
    st.rerun()
