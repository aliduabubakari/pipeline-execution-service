from __future__ import annotations
from typing import Optional
from pydantic import BaseModel


class UploadResponse(BaseModel):
    installed: dict[str, list[str]]
    discovered_dags: list[str] = []   # DAG IDs Airflow registered after the upload
    generated_dags: list[str] = []
    package_mode: str = "legacy"


class TriggerRequest(BaseModel):
    dag_id: str
    conf: dict = {}


class TriggerResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str
    logical_date: Optional[str] = None


class RunStatusResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
