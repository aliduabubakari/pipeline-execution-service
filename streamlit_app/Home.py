"""
Pipeline Execution Service — Streamlit Frontend
Entry point: redirects straight to the Run Pipeline page.
"""
import streamlit as st

st.set_page_config(
    page_title="Pipeline Execution Service",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── shared CSS injected once here, inherited by all pages ──────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }

    /* ── dark canvas ── */
    .stApp {
        background-color: #0d0f12;
        color: #e2e8f0;
    }

    /* ── sidebar ── */
    section[data-testid="stSidebar"] {
        background-color: #111318 !important;
        border-right: 1px solid #1e2330;
    }
    section[data-testid="stSidebar"] * {
        color: #94a3b8 !important;
    }
    section[data-testid="stSidebar"] .st-emotion-cache-1v0mbdj,
    section[data-testid="stSidebar"] a {
        color: #e2e8f0 !important;
    }

    /* ── headings ── */
    h1 { font-family: 'IBM Plex Mono', monospace !important; font-size: 1.6rem !important; color: #f8fafc !important; letter-spacing: -0.02em; }
    h2 { font-family: 'IBM Plex Mono', monospace !important; font-size: 1.1rem !important; color: #cbd5e1 !important; letter-spacing: 0.04em; text-transform: uppercase; }
    h3 { font-family: 'IBM Plex Sans', sans-serif !important; font-size: 0.95rem !important; color: #94a3b8 !important; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; }

    /* ── metric cards ── */
    [data-testid="metric-container"] {
        background: #151820 !important;
        border: 1px solid #1e2330 !important;
        border-radius: 4px !important;
        padding: 20px 24px !important;
    }
    [data-testid="metric-container"] label {
        color: #64748b !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #f0f6ff !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 1.8rem !important;
    }

    /* ── buttons ── */
    .stButton > button {
        background: #1d4ed8 !important;
        color: #fff !important;
        border: none !important;
        border-radius: 3px !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.8rem !important;
        letter-spacing: 0.06em;
        padding: 10px 24px !important;
        transition: background 0.15s;
    }
    .stButton > button:hover { background: #2563eb !important; }
    .stButton > button:disabled { background: #1e2330 !important; color: #475569 !important; }

    /* ── file uploader ── */
    [data-testid="stFileUploader"] {
        border: 1px dashed #2d3748 !important;
        border-radius: 4px !important;
        background: #111318 !important;
    }

    /* ── selectbox / inputs ── */
    .stSelectbox > div > div {
        background: #151820 !important;
        border-color: #1e2330 !important;
        color: #e2e8f0 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.85rem !important;
    }

    /* ── dataframe ── */
    [data-testid="stDataFrame"] {
        border: 1px solid #1e2330 !important;
        border-radius: 4px !important;
    }

    /* ── status badges ── */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 2px;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }
    .badge-success  { background: #052e16; color: #4ade80; border: 1px solid #166534; }
    .badge-running  { background: #431407; color: #fb923c; border: 1px solid #9a3412; }
    .badge-queued   { background: #1e1b4b; color: #a5b4fc; border: 1px solid #3730a3; }
    .badge-failed   { background: #2d0a0a; color: #f87171; border: 1px solid #991b1b; }
    .badge-unknown  { background: #1a1a1a; color: #64748b; border: 1px solid #334155; }

    /* ── dividers ── */
    hr { border-color: #1e2330 !important; }

    /* ── expander ── */
    details { border: 1px solid #1e2330 !important; border-radius: 4px !important; background: #111318 !important; }

    /* ── info/success/warning/error boxes ── */
    .stAlert { border-radius: 3px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.82rem !important; }

    /* ── code ── */
    code, pre { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.8rem !important; }

    /* ── tabs ── */
    .stTabs [data-baseweb="tab-list"] { gap: 2px; border-bottom: 1px solid #1e2330; }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        color: #64748b !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        padding: 8px 18px !important;
        border-radius: 0 !important;
    }
    .stTabs [aria-selected="true"] {
        color: #f0f6ff !important;
        border-bottom: 2px solid #3b82f6 !important;
    }

    /* hide default streamlit chrome */
    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("## ⬡ Pipeline Execution Service")
st.markdown("Select a page from the sidebar to get started.")

col1, col2 = st.columns(2)
with col1:
    st.markdown(
        """
        <div style="background:#151820;border:1px solid #1e2330;border-radius:4px;padding:28px 24px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:10px;">STEP 01</div>
          <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;color:#f0f6ff;margin-bottom:8px;">Run Pipeline →</div>
          <div style="font-size:0.85rem;color:#94a3b8;line-height:1.6;">Upload a <code>.zip</code> package, select a DAG, trigger a run, and download the output CSV.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with col2:
    st.markdown(
        """
        <div style="background:#151820;border:1px solid #1e2330;border-radius:4px;padding:28px 24px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#64748b;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:10px;">STEP 02</div>
          <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;color:#f0f6ff;margin-bottom:8px;">Metrics →</div>
          <div style="font-size:0.85rem;color:#94a3b8;line-height:1.6;">Live Prometheus metrics: task duration, exit codes, carbon emissions, and container resource usage.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )