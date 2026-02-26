"""
Page 2 — Metrics
Uses backend metrics endpoints for normalized task summaries and range series.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Metrics · PES",
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
    .stAlert { border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.82rem !important; }
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
        <div style="margin: 24px 0 12px;">
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
                    padding:18px 20px;color:#64748b;font-family:'IBM Plex Mono',monospace;font-size:0.78rem;">
          {msg}
        </div>
        """,
        unsafe_allow_html=True,
    )


def fmt_num(value, digits: int = 3) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
    except Exception:
        return "—"
    if abs(v) >= 1:
        return f"{v:.{digits}f}"
    return f"{v:.6g}"


def _container_display_name(metric: dict) -> str:
    for key in (
        "container_label_com_docker_compose_service",
        "container",
        "name",
        "container_name",
    ):
        value = metric.get(key)
        if value and value not in {"POD", "/"}:
            return str(value)
    # Last resort: shorten container id if present.
    cid = metric.get("id")
    if isinstance(cid, str) and cid.startswith("/docker/"):
        return cid.split("/")[-1][:12]
    return "unknown"


def _docker_id_from_cadvisor_metric(metric: dict) -> str | None:
    cid = metric.get("id")
    if isinstance(cid, str) and cid.startswith("/docker/"):
        return cid.split("/")[-1]
    return None


if "metrics_last_refresh" not in st.session_state:
    st.session_state.metrics_last_refresh = time.time()

header_left, header_right = st.columns([6, 2])
with header_left:
    st.markdown("# Metrics")
    st.markdown("## Task-level summaries and trends from the Execution API")
with header_right:
    refresh_now = st.button("↻  Refresh now")

st.divider()

# Sidebar filters
try:
    pipelines = api.list_pipelines()
    dag_ids = [p["dag_id"] for p in pipelines]
except Exception as e:
    dag_ids = []
    st.warning(f"Could not load DAG list from API: {e}")

with st.sidebar:
    st.markdown("### Filters")
    selected_dag = st.selectbox("DAG", dag_ids if dag_ids else ["— no DAGs —"], index=0 if dag_ids else 0)
    window = st.selectbox("Window", ["15m", "1h", "6h", "24h"], index=1)
    step = st.selectbox("Step", ["15s", "30s", "60s"], index=1)
    auto_refresh = st.checkbox("Auto-refresh every 15s", value=True)
    compose_project_filter = st.text_input("Compose project (optional)", value="pipeline-execution-service")

# Capabilities
capabilities = None
try:
    capabilities = api.metrics_capabilities()
except Exception as e:
    st.warning(f"Capabilities endpoint unavailable: {e}")

if capabilities:
    providers = capabilities.get("providers", {})
    c1, c2, c3 = st.columns(3)
    c1.metric("Prometheus", "up" if providers.get("prometheus", {}).get("reachable") else "down")
    c2.metric("Airflow", "up" if providers.get("airflow", {}).get("reachable") else "down")
    c3.metric("Task Metrics", len(capabilities.get("task_metrics", [])))

tab_latest, tab_trends, tab_platform = st.tabs(["Latest Run", "Task Trends", "Platform"])

selected_dag_valid = bool(dag_ids) and selected_dag != "— no DAGs —"

with tab_latest:
    section("Latest Run Summary", "Airflow latest run + task status merged with Prometheus task metrics")
    if not selected_dag_valid:
        no_data_card("No DAG selected.")
    else:
        try:
            latest = api.task_metrics_latest_with_airflow(selected_dag)
            tasks = latest.get("tasks", [])
        except Exception as e:
            latest = None
            tasks = []
            st.error(f"Could not load latest task metrics: {e}")

        if not tasks:
            no_data_card("No task metrics found yet for this DAG. Trigger a run first.")
        else:
            top1, top2, top3, top4 = st.columns(4)
            top1.metric("DAG Run", latest.get("dag_run_state", "—") if latest else "—")
            top2.metric("Tasks", len(tasks))
            top3.metric("Succeeded", sum(1 for t in tasks if t.get("airflow_state") == "success"))
            top4.metric("Failed", sum(1 for t in tasks if t.get("airflow_state") == "failed"))

            rows = []
            for t in tasks:
                rows.append(
                    {
                        "task_id": t.get("task_id"),
                        "airflow_state": t.get("airflow_state"),
                        "try_number": t.get("try_number"),
                        "duration_seconds": t.get("duration_seconds"),
                        "exit_code": t.get("exit_code"),
                        "emissions_kg_co2": t.get("emissions_kg_co2"),
                        "start_date": t.get("start_date"),
                        "end_date": t.get("end_date"),
                    }
                )
            df_latest = pd.DataFrame(rows)
            st.dataframe(df_latest, use_container_width=True, hide_index=True)

            section("Run Metadata")
            st.code(
                (
                    f"dag_id={latest.get('dag_id')}\n"
                    f"run_id={latest.get('run_id')}\n"
                    f"dag_run_state={latest.get('dag_run_state')}"
                ),
                language="text",
            )

with tab_trends:
    section("Task Metric Time Series", "Prometheus-backed range queries via execution-api")
    if not selected_dag_valid:
        no_data_card("No DAG selected.")
    else:
        metric_choice = st.selectbox(
            "Metric",
            [
                "pipeline_task_duration_seconds",
                "pipeline_task_exit_code",
                "pipeline_task_emissions_kg_co2",
            ],
            index=0,
        )

        task_options = []
        try:
            latest_rows = api.task_metrics_latest(selected_dag).get("tasks", [])
            task_options = [t["task_id"] for t in latest_rows]
        except Exception:
            task_options = []

        selected_task = st.selectbox(
            "Task (optional)",
            ["All tasks"] + task_options,
            index=0,
        )

        try:
            range_payload = api.task_metrics_range(
                dag_id=selected_dag,
                task_id=None if selected_task == "All tasks" else selected_task,
                metric_name=metric_choice,
                window=window,
                step=step,
            )
            series = range_payload.get("series", [])
        except Exception as e:
            series = []
            st.error(f"Could not load range metrics: {e}")

        if not series:
            no_data_card("No range data found for the selected filters. Try a wider window or run the DAG again.")
        else:
            chart_rows = []
            for s in series:
                label = f"{s.get('task_id')} ({s.get('metric_name').replace('pipeline_task_', '')})"
                for p in s.get("points", []):
                    chart_rows.append(
                        {
                            "timestamp": datetime.fromtimestamp(float(p["ts"]), tz=timezone.utc),
                            "series": label,
                            "value": float(p["value"]),
                        }
                    )
            df_chart = pd.DataFrame(chart_rows)
            if not df_chart.empty:
                pivot = df_chart.pivot_table(index="timestamp", columns="series", values="value", aggfunc="last").sort_index()
                st.line_chart(pivot, height=320, use_container_width=True)
            with st.expander("Raw range series"):
                st.json(range_payload)

with tab_platform:
    section("Platform Health", "Curated views for cAdvisor and Airflow StatsD to reduce raw metric dump noise")

    p1, p2 = st.columns(2)
    with p1:
        st.markdown("#### cAdvisor (containers)")
        # cAdvisor label sets vary by version/runtime; do not rely on `name` existing.
        cpu_results = prom.query('rate(container_cpu_usage_seconds_total{id!="/"}[1m])')
        mem_results = prom.query('container_memory_working_set_bytes{id!="/"}')
        if not cpu_results and not mem_results:
            no_data_card(
                "No container data returned. cAdvisor may be up but task containers are short-lived (`auto_remove=True`). "
                "Use task-level metrics for post-run analysis and watch this tab during active runs."
            )
        else:
            container_map = {}
            try:
                inventory = api.platform_containers(compose_project=compose_project_filter or None)
                if inventory.get("reachable"):
                    for c in inventory.get("containers", []):
                        cid = str(c.get("id", ""))
                        if not cid:
                            continue
                        label = c.get("compose_service") or c.get("name") or cid[:12]
                        container_map[cid] = label
                        container_map[cid[:12]] = label
            except Exception:
                inventory = None

            cpu_by_name = {}
            for r in cpu_results:
                metric = r.get("metric", {})
                docker_id = _docker_id_from_cadvisor_metric(metric)
                name = (
                    container_map.get(docker_id or "", None)
                    or container_map.get((docker_id or "")[:12], None)
                    or _container_display_name(metric)
                )
                try:
                    cpu_by_name[name] = cpu_by_name.get(name, 0.0) + float(r["value"][1]) * 100.0
                except Exception:
                    continue

            mem_by_name = {}
            for r in mem_results:
                metric = r.get("metric", {})
                docker_id = _docker_id_from_cadvisor_metric(metric)
                name = (
                    container_map.get(docker_id or "", None)
                    or container_map.get((docker_id or "")[:12], None)
                    or _container_display_name(metric)
                )
                try:
                    # Keep the max memory series if duplicate labels collapse to same display name.
                    mem_mb = float(r["value"][1]) / (1024 ** 2)
                    mem_by_name[name] = max(mem_by_name.get(name, 0.0), mem_mb)
                except Exception:
                    continue

            names = sorted(
                n
                for n in set(cpu_by_name) | set(mem_by_name)
                if n and n not in {"unknown"} and n not in {"/docker", "/restricted"}
            )
            rows = []
            for name in names:
                rows.append(
                    {
                        "container": name,
                        "cpu_pct": round(cpu_by_name.get(name, 0.0), 3),
                        "mem_mb": round(mem_by_name.get(name, 0.0), 2),
                    }
                )
            if not rows:
                no_data_card(
                    "cAdvisor returned series, but no displayable container labels were found. "
                    "Check Prometheus labels for `container_memory_working_set_bytes` in Prometheus UI."
                )
            else:
                df_c = pd.DataFrame(rows).sort_values("mem_mb", ascending=False)
                st.dataframe(df_c.head(12), use_container_width=True, hide_index=True)
                if not container_map:
                    st.caption(
                        "Showing cAdvisor labels/IDs only. Enable execution-api Docker socket access and `/platform/containers` "
                        "for friendlier Docker Compose service names."
                    )

    with p2:
        st.markdown("#### Airflow / StatsD (curated)")
        curated = {
            "Scheduler Heartbeat": "airflow_scheduler_heartbeat_total",
            "Dagbag Size": "airflow_dagbag_size",
            "Executor Open Slots": "airflow_executor_open_slots",
            "Executor Queued Tasks": "airflow_executor_queued_tasks",
            "Executor Running Tasks": "airflow_executor_running_tasks",
        }
        metric_rows = []
        for label, q in curated.items():
            res = prom.query(q)
            if not res:
                continue
            try:
                metric_rows.append({"metric": label, "value": float(res[0]["value"][1])})
            except Exception:
                continue
        if not metric_rows:
            no_data_card("No curated Airflow metrics found yet. StatsD exporter may still be warming up.")
        else:
            df_air = pd.DataFrame(metric_rows)
            st.dataframe(df_air, use_container_width=True, hide_index=True)

        with st.expander("Airflow metric series (searchable, reduced dump)"):
            airflow_series = prom.query('{job="statsd-exporter"}')
            if not airflow_series:
                st.caption("No statsd-exporter series available.")
            else:
                rows = []
                for r in airflow_series:
                    rows.append(
                        {
                            "metric_name": r["metric"].get("__name__", "?"),
                            "value": r["value"][1],
                            "labels": {k: v for k, v in r["metric"].items() if k != "__name__"},
                        }
                    )
                st.dataframe(pd.DataFrame(rows).sort_values("metric_name"), use_container_width=True, hide_index=True)

st.divider()
elapsed = int(time.time() - st.session_state.metrics_last_refresh)
remaining = max(0, 15 - elapsed)
st.caption(f"Execution API + Prometheus backed metrics · next refresh in {remaining}s")

if refresh_now or (auto_refresh and elapsed >= 15):
    st.session_state.metrics_last_refresh = time.time()
    st.rerun()
