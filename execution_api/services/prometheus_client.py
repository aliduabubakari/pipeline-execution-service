from __future__ import annotations

import requests


class PrometheusClient:
    def __init__(self, base_url: str, timeout_s: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    def query(self, promql: str) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/query",
            params={"query": promql},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        payload = r.json()
        return payload.get("data", {}).get("result", [])

    def query_range(self, promql: str, *, start: str, end: str, step: str = "30s") -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": step},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        payload = r.json()
        return payload.get("data", {}).get("result", [])
