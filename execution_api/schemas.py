from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    installed: dict[str, list[str]]
    discovered_dags: list[str] = []   # DAG IDs Airflow registered after the upload
    generated_dags: list[str] = []
    package_mode: str = "legacy"


class TriggerRequest(BaseModel):
    dag_id: str
    conf: dict = Field(default_factory=dict)


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


class TaskMetricsLatest(BaseModel):
    dag_id: str
    task_id: str
    key: str
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None
    emissions_kg_co2: Optional[float] = None


class TaskMetricsLatestResponse(BaseModel):
    tasks: list[TaskMetricsLatest]


class TaskMetricsLatestWithAirflow(BaseModel):
    dag_id: str
    task_id: str
    key: str
    run_id: Optional[str] = None
    dag_run_state: Optional[str] = None
    airflow_state: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    try_number: Optional[int] = None
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None
    emissions_kg_co2: Optional[float] = None


class TaskMetricsLatestWithAirflowResponse(BaseModel):
    dag_id: str
    run_id: Optional[str] = None
    dag_run_state: Optional[str] = None
    tasks: list[TaskMetricsLatestWithAirflow]


class MetricsCapabilitiesResponse(BaseModel):
    providers: dict
    endpoints: dict[str, str]
    task_metrics: list[str]
    task_range_defaults: dict[str, str]


class TaskMetricRangePoint(BaseModel):
    ts: float
    value: float


class TaskMetricRangeSeries(BaseModel):
    metric_name: str
    dag_id: str
    task_id: str
    key: str
    points: list[TaskMetricRangePoint]


class TaskMetricsRangeResponse(BaseModel):
    start: str
    end: str
    step: str
    filters: dict[str, Optional[str]]
    series: list[TaskMetricRangeSeries]


class ContainerInventoryItem(BaseModel):
    id: str
    name: Optional[str] = None
    status: Optional[str] = None
    image: Optional[str] = None
    compose_project: Optional[str] = None
    compose_service: Optional[str] = None


class ContainerInventoryResponse(BaseModel):
    reachable: bool
    containers: list[ContainerInventoryItem]
    error: Optional[str] = None
