"""
Shared configuration and thin API/Prometheus clients.
Import from any page:  from utils import api, prom, cfg
"""
from __future__ import annotations

import os
import requests
from dataclasses import dataclass, field


# ── config ─────────────────────────────────────────────────────────────────
@dataclass
class Config:
    api_base: str    = field(default_factory=lambda: os.environ.get("EXECUTION_API_URL", "http://localhost:8090"))
    prom_base: str   = field(default_factory=lambda: os.environ.get("PROMETHEUS_URL",    "http://localhost:9090"))
    poll_interval: int = 3   # seconds between status polls


cfg = Config()


# ── execution API client ────────────────────────────────────────────────────
class APIClient:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def upload(self, zip_bytes: bytes, filename: str) -> dict:
        r = requests.post(
            f"{self.base}/upload?replace=true",
            files={"file": (filename, zip_bytes, "application/zip")},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def list_pipelines(self) -> list[dict]:
        r = requests.get(f"{self.base}/pipelines", timeout=10)
        r.raise_for_status()
        return r.json().get("pipelines", [])

    def trigger(self, dag_id: str, conf: dict | None = None) -> dict:
        r = requests.post(
            f"{self.base}/pipelines/trigger",
            json={"dag_id": dag_id, "conf": conf or {}},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def run_status(self, dag_id: str, run_id: str) -> dict:
        r = requests.get(
            f"{self.base}/pipelines/{dag_id}/runs/{requests.utils.quote(run_id, safe='')}",
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def download_output(self, path: str = "/mnt/data/output.csv") -> bytes | None:
        """
        Tries to fetch output.csv via a /files endpoint.
        Falls back to None so the page can show a graceful message.
        The execution API will need a GET /files/{filename} endpoint for this
        (or you can mount the data volume into the Streamlit container directly).
        """
        try:
            r = requests.get(f"{self.base}/files/output.csv", timeout=10)
            r.raise_for_status()
            return r.content
        except Exception:
            return None


# ── prometheus client ───────────────────────────────────────────────────────
class PromClient:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def query(self, promql: str) -> list[dict]:
        """Instant query → returns result list or []."""
        try:
            r = requests.get(
                f"{self.base}/api/v1/query",
                params={"query": promql},
                timeout=10,
            )
            r.raise_for_status()
            return r.json().get("data", {}).get("result", [])
        except Exception:
            return []

    def query_range(self, promql: str, start: str, end: str, step: str = "30s") -> list[dict]:
        """Range query → returns result list or []."""
        try:
            r = requests.get(
                f"{self.base}/api/v1/query_range",
                params={"query": promql, "start": start, "end": end, "step": step},
                timeout=15,
            )
            r.raise_for_status()
            return r.json().get("data", {}).get("result", [])
        except Exception:
            return []

    def scalar(self, promql: str, default: str = "—") -> str:
        """Return first value as formatted string, or default."""
        results = self.query(promql)
        if not results:
            return default
        try:
            val = float(results[0]["value"][1])
            # auto-format
            if val == int(val):
                return str(int(val))
            return f"{val:.3f}"
        except Exception:
            return default


api  = APIClient(cfg.api_base)
prom = PromClient(cfg.prom_base)