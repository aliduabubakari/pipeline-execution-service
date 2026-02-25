from __future__ import annotations

import time
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

    def list_dag_runs(
        self,
        dag_id: str,
        *,
        limit: int = 1,
        order_by: str = "-start_date",
    ) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            auth=self.auth,
            params={"limit": limit, "order_by": order_by},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json().get("dag_runs", [])

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            auth=self.auth,
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json().get("task_instances", [])

    def wait_for_dag(
        self,
        dag_id: str,
        timeout_s: int = 60,
        poll_interval_s: int = 3,
    ) -> bool:
        """
        Poll until the given dag_id appears in Airflow's registered DAGs.
        Returns True if found within timeout, False otherwise.

        Airflow rescans DAG_DIR every DAG_DIR_LIST_INTERVAL seconds (set to 15s).
        This gives it up to `timeout_s` seconds to pick up a freshly copied file.
        """
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                dags = self.list_dags()
                if any(d["dag_id"] == dag_id for d in dags):
                    return True
            except Exception:
                pass  # Airflow might be briefly unavailable — keep retrying
            time.sleep(poll_interval_s)
        return False
