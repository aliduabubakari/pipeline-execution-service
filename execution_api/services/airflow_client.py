from __future__ import annotations

import requests


class AirflowClient:
    def __init__(self, base_url: str, username: str, password: str, timeout_s: int = 15):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.timeout_s = timeout_s

    def health(self) -> dict:
        r = requests.get(f"{self.base_url}/health", timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def list_dags(self) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/dags",
            auth=self.auth,
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json().get("dags", [])

    def unpause_dag(self, dag_id: str) -> dict:
        r = requests.patch(
            f"{self.base_url}/api/v1/dags/{dag_id}",
            auth=self.auth,
            json={"is_paused": False},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json()

    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> dict:
        r = requests.post(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            auth=self.auth,
            json={"conf": conf or {}},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json()

    def get_dag_run(self, dag_id: str, run_id: str) -> dict:
        r = requests.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
            auth=self.auth,
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json()