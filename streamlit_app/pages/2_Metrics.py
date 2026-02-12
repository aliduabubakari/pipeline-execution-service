"""
Page 2 — Metrics
Live Prometheus data: task duration, exit codes, carbon emissions, container CPU/memory.
Auto-refreshes every 15 s via st.rerun().
"""
from __future__ import annotations

import time
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
    h1 { font-family: 'IBM Plex Mono', monospace !important; font-size: 1.5rem !important; color: #f8fafc !important; }
    h2 { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.85rem !important; color: #64748b !important; letter-spacing: 0.1em; text-transform: uppercase; }
    [data-testid="metric-container"] { background: #151820 !important; border: 1px solid #1e2330 !important; border-radius: 4px !important; padding: 20px 24px !important; }
    [data-testid="metric-container"] label { color: #64748b !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.68rem !important; text-transform: uppercase; letter-spacing: 0.1em; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #f0f6ff !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.7rem !important; }
    .stButton > button { background: #151820 !important; color: #94a3b8 !important; border: 1px solid #1e2330 !important; border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.75rem !important; letter-spacing: 0.06em; padding: 6px 16px !important; }
    .stButton > button:hover { border-color: #3b82f6 !important; color: #e2e8f0 !important; }
    [data-testid="stDataFrame"] { border: 1px solid #1e2330 !important; border-radius: 4px !important; }
    .stAlert { border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.82rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 2px; border-bottom: 1px solid #1e2330; }
    .stTabs [data-baseweb="tab"] { background: transparent !important; color: #64748b !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.73rem !important; letter-spacing: 0.06em; text-transform: uppercase; padding: 8px 18px !important; border-radius: 0 !important; }
    .stTabs [aria-selected="true"] { color: #f0f6ff !important; border-bottom: 2px solid #3b82f6 !important; }
    hr { border-color: #1e2330 !important; }
    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils import prom


# ── section label helper ─────────────────────────────────────────────────────
def section(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div style="margin: 32px 0 16px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;
                      color:#3b82f6;letter-spacing:0.12em;text-transform:uppercase;margin-bottom:4px;">
            {title}
          </div>
          {'<div style="font-size:0.82rem;color:#64748b;">' + subtitle + '</div>' if subtitle else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


def no_data_card(msg: str = "No data yet — trigger a pipeline run first.") -> None:
    st.markdown(
        f"""
        <div style="background:#111318;border:1px dashed #1e2330;border-radius:4px;
                    padding:20px 24px;font-family:'IBM Plex Mono',monospace;
                    font-size:0.78rem;color:#475569;text-align:center;">
          {msg}
        </div>
        """,
        unsafe_allow_html=True,
    )


def metric_card(label: str, value: str, unit: str = "", accent: str = "#3b82f6") -> str:
    return f"""
    <div style="background:#111318;border:1px solid #1e2330;border-radius:4px;padding:20px 22px;height:100%;">
      <div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;color:#64748b;
                  letter-spacing:0.1em;text-transform:uppercase;margin-bottom:10px;">{label}</div>
      <div style="font-family:'IBM Plex Mono',monospace;font-size:1.9rem;color:{accent};
                  line-height:1;">{value}</div>
      <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#475569;
                  margin-top:4px;">{unit}</div>
    </div>
    """


# ── page header ──────────────────────────────────────────────────────────────
top_left, top_right = st.columns([6, 2])
with top_left:
    st.markdown("# Metrics")
    st.markdown("## Live Prometheus · Auto-refresh every 15 s")
with top_right:
    st.markdown("<div style='margin-top:32px;'></div>", unsafe_allow_html=True)
    refresh_btn = st.button("↻  Refresh now")
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = time.time()

st.divider()


# ── tabs ─────────────────────────────────────────────────────────────────────
tab_pipeline, tab_carbon, tab_containers, tab_airflow = st.tabs([
    "Pipeline Tasks",
    "Carbon Emissions",
    "Containers",
    "Airflow",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — PIPELINE TASKS
# ════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    section("Pipeline Task Metrics", "From node-exporter textfile collector")

    duration_results = prom.query("pipeline_task_duration_seconds")
    exitcode_results = prom.query("pipeline_task_exit_code")

    if not duration_results and not exitcode_results:
        no_data_card()
    else:
        # one card per (dag_id, task_id) pair
        all_series = {
            (r["metric"].get("dag_id", "?"), r["metric"].get("task_id", "?")): r
            for r in duration_results
        }
        exit_by_key = {
            (r["metric"].get("dag_id", "?"), r["metric"].get("task_id", "?")): r
            for r in exitcode_results
        }

        for (dag_id, task_id), series in all_series.items():
            duration_val = float(series["value"][1])
            exit_series  = exit_by_key.get((dag_id, task_id))
            exit_val     = int(float(exit_series["value"][1])) if exit_series else None
            exit_label   = "✓ 0 — OK" if exit_val == 0 else (f"✗ {exit_val} — FAILED" if exit_val is not None else "—")
            exit_color   = "#4ade80" if exit_val == 0 else ("#f87171" if exit_val else "#94a3b8")

            st.markdown(
                f"""
                <div style="background:#0f1521;border:1px solid #1e2330;border-radius:4px;
                            padding:18px 22px;margin-bottom:12px;">
                  <div style="display:flex;gap:8px;align-items:center;margin-bottom:14px;">
                    <span style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;
                                 color:#3b82f6;background:#0a1628;border:1px solid #1e3a5f;
                                 padding:2px 8px;border-radius:2px;">{dag_id}</span>
                    <span style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;
                                 color:#94a3b8;">/ {task_id}</span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
                    <div>
                      <div style="font-family:'IBM Plex Mono',monospace;font-size:0.63rem;
                                  color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:4px;">Duration</div>
                      <div style="font-family:'IBM Plex Mono',monospace;font-size:1.6rem;
                                  color:#f0f6ff;">{duration_val:.2f}<span style="font-size:0.8rem;color:#475569;margin-left:4px;">s</span></div>
                    </div>
                    <div>
                      <div style="font-family:'IBM Plex Mono',monospace;font-size:0.63rem;
                                  color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:4px;">Exit Code</div>
                      <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;
                                  color:{exit_color};margin-top:6px;">{exit_label}</div>
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # raw table expander
    if duration_results or exitcode_results:
        with st.expander("Raw metric values"):
            rows = []
            for r in duration_results:
                rows.append({
                    "metric": "pipeline_task_duration_seconds",
                    "dag_id":  r["metric"].get("dag_id"),
                    "task_id": r["metric"].get("task_id"),
                    "value":   r["value"][1],
                })
            for r in exitcode_results:
                rows.append({
                    "metric": "pipeline_task_exit_code",
                    "dag_id":  r["metric"].get("dag_id"),
                    "task_id": r["metric"].get("task_id"),
                    "value":   r["value"][1],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — CARBON EMISSIONS
# ════════════════════════════════════════════════════════════════════════════
with tab_carbon:
    section("Carbon Emissions", "Estimated via CodeCarbon — kg CO₂ equivalent")

    emissions_results = prom.query("pipeline_task_emissions_kg_co2")

    if not emissions_results:
        no_data_card("No emissions data yet. Enable CodeCarbon (CODECARBON_SAVE_TO_FILE=true) and trigger a run.")
    else:
        total_kg   = sum(float(r["value"][1]) for r in emissions_results)
        total_g    = total_kg * 1000
        km_eq      = total_kg / 0.00021   # ~0.21 kg CO2 per km driving
        kwh_eq     = total_kg / 0.233     # US avg 0.233 kg CO2 per kWh

        # summary cards
        c1, c2, c3, c4 = st.columns(4)
        cards = [
            (c1, "Total Emissions", f"{total_kg:.6f}", "kg CO₂e", "#4ade80"),
            (c2, "In Grams",        f"{total_g:.4f}",  "g CO₂e",  "#34d399"),
            (c3, "≈ Driving",       f"{km_eq:.2f}",    "metres",  "#6ee7b7"),
            (c4, "≈ Energy",        f"{kwh_eq:.4f}",   "kWh",     "#a7f3d0"),
        ]
        for col, label, val, unit, accent in cards:
            with col:
                st.markdown(metric_card(label, val, unit, accent), unsafe_allow_html=True)

        st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)

        # per-task breakdown
        section("Per-task breakdown")
        rows = []
        for r in emissions_results:
            kg = float(r["value"][1])
            rows.append({
                "dag_id":   r["metric"].get("dag_id", "?"),
                "task_id":  r["metric"].get("task_id", "?"),
                "kg_co2e":  kg,
                "g_co2e":   kg * 1000,
                "% of total": f"{100 * kg / total_kg:.1f}%" if total_kg else "—",
            })
        df_em = pd.DataFrame(rows).sort_values("kg_co2e", ascending=False)
        st.dataframe(df_em, use_container_width=True, hide_index=True)

        # equivalence context callout
        st.markdown(
            f"""
            <div style="background:#052e16;border:1px solid #166534;border-radius:4px;
                        padding:16px 20px;margin-top:20px;font-size:0.83rem;color:#86efac;
                        font-family:'IBM Plex Mono',monospace;">
              🌱 &nbsp;{total_g:.6f} g CO₂e — roughly equivalent to charging a smartphone
              for <strong>{total_g / 8.22 * 60:.1f} seconds</strong>
              (≈ 8.22 g CO₂e per full charge).
            </div>
            """,
            unsafe_allow_html=True,
        )


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — CONTAINERS (cAdvisor)
# ════════════════════════════════════════════════════════════════════════════
with tab_containers:
    section("Container Resource Usage", "cAdvisor → Prometheus · last-known values")

    # CPU — rate of cpu seconds consumed
    cpu_results = prom.query(
        'rate(container_cpu_usage_seconds_total{name!=""}[1m])'
    )
    # Memory working set
    mem_results = prom.query(
        'container_memory_working_set_bytes{name!=""}'
    )

    if not cpu_results and not mem_results:
        no_data_card("No container data — is cAdvisor running? Check http://localhost:8081")
    else:
        cpu_by_name = {r["metric"].get("name", "?"): float(r["value"][1]) for r in cpu_results}
        mem_by_name = {r["metric"].get("name", "?"): float(r["value"][1]) for r in mem_results}
        all_names = sorted(set(list(cpu_by_name) + list(mem_by_name)))

        # filter out pause/infra containers
        all_names = [n for n in all_names if n and not n.startswith("k8s_POD")]

        rows = []
        for name in all_names:
            cpu_pct = cpu_by_name.get(name, 0) * 100
            mem_mb  = mem_by_name.get(name, 0) / (1024 ** 2)
            rows.append({
                "container": name,
                "cpu_%":     round(cpu_pct, 3),
                "mem_MB":    round(mem_mb, 1),
            })

        df_c = pd.DataFrame(rows).sort_values("mem_MB", ascending=False)

        # top 3 by memory — visual cards
        top3 = df_c.head(3)
        cols_c = st.columns(len(top3))
        for i, (_, row) in enumerate(top3.iterrows()):
            short_name = row["container"].replace("compose-", "").replace("-1", "")
            with cols_c[i]:
                st.markdown(
                    metric_card(short_name, f"{row['mem_MB']:.0f}", "MB RAM", "#6366f1"),
                    unsafe_allow_html=True,
                )

        st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
        section("All containers")
        st.dataframe(df_c, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — AIRFLOW (StatsD)
# ════════════════════════════════════════════════════════════════════════════
with tab_airflow:
    section("Airflow Internal Metrics", "Via StatsD exporter → Prometheus")

    # common airflow statsd metrics
    queries = {
        "dag_processing.total_parse_time": "airflow_dag_processing_total_parse_time",
        "scheduler.heartbeat":             "airflow_scheduler_heartbeat_total",
        "dagbag_size":                     "airflow_dagbag_size",
        "executor.open_slots":             "airflow_executor_open_slots",
        "executor.queued_tasks":           "airflow_executor_queued_tasks",
        "executor.running_tasks":          "airflow_executor_running_tasks",
    }

    found = {}
    for label, promql in queries.items():
        results = prom.query(promql)
        if results:
            found[label] = float(results[0]["value"][1])

    # also try prefix-agnostic search
    all_airflow = prom.query('{job="statsd-exporter"}')

    if not found and not all_airflow:
        no_data_card(
            "No Airflow StatsD metrics found yet.<br>"
            "Confirm <code>AIRFLOW__METRICS__STATSD_ON=true</code> and that statsd-exporter is reachable."
        )
    else:
        if found:
            n = len(found)
            cols_a = st.columns(min(n, 3))
            for i, (label, val) in enumerate(found.items()):
                with cols_a[i % 3]:
                    st.metric(label=label, value=f"{val:g}")

        # full metric dump
        if all_airflow:
            st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)
            with st.expander(f"All StatsD metrics ({len(all_airflow)} series)"):
                rows_a = []
                for r in all_airflow:
                    rows_a.append({
                        "metric_name": r["metric"].get("__name__", "?"),
                        "labels":      str({k: v for k, v in r["metric"].items() if k != "__name__"}),
                        "value":       r["value"][1],
                    })
                st.dataframe(
                    pd.DataFrame(rows_a).sort_values("metric_name"),
                    use_container_width=True,
                    hide_index=True,
                )


# ── footer + auto-refresh ────────────────────────────────────────────────────
st.divider()
elapsed = int(time.time() - st.session_state.last_refresh)
remaining = max(0, 15 - elapsed)

footer_col, timer_col = st.columns([5, 2])
with footer_col:
    st.markdown(
        """
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#334155;">
          Data sourced from Prometheus · <a href="http://localhost:9090" target="_blank"
          style="color:#3b82f6;text-decoration:none;">Open Prometheus UI →</a>
          &nbsp;|&nbsp;
          <a href="http://localhost:3000" target="_blank"
          style="color:#3b82f6;text-decoration:none;">Open Grafana →</a>
        </div>
        """,
        unsafe_allow_html=True,
    )
with timer_col:
    st.markdown(
        f"""
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#334155;text-align:right;">
          next refresh in {remaining} s
        </div>
        """,
        unsafe_allow_html=True,
    )

if refresh_btn or elapsed >= 15:
    st.session_state.last_refresh = time.time()
    st.rerun()