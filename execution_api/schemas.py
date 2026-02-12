from __future__ import annotations

from typing import Any
from pydantic import BaseModel


class UploadResponse(BaseModel):
    installed: dict[str, list[str]]


class TriggerRequest(BaseModel):
    dag_id: str
    conf: dict[str, Any] | None = None


class TriggerResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str
    logical_date: str


class RunStatusResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str          # queued | running | success | failed
    start_date: str | None
    end_date: str | None