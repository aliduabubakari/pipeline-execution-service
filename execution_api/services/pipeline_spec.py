from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from execution_api.utils.fs import PackageError


DEFAULT_RUNNER_IMAGE = "pipeline-task-runner:latest"
DEFAULT_DOCKER_NETWORK = "app_network"


@dataclass
class TaskSpec:
    task_id: str
    operator: str = "docker"
    script: str | None = None
    image: str = DEFAULT_RUNNER_IMAGE
    env: dict[str, str] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    metrics_enabled: bool = True
    carbon_enabled: bool = True


@dataclass
class PipelineSpec:
    dag_id: str
    tasks: list[TaskSpec]
    schedule: str | None = None
    tags: list[str] = field(default_factory=lambda: ["runner", "generated"])
    description: str | None = None


def load_pipeline_specs(manifest_path: Path) -> list[PipelineSpec]:
    raw = _load_manifest_file(manifest_path)

    if isinstance(raw, dict) and "pipelines" in raw:
        items = raw["pipelines"]
        if not isinstance(items, list) or not items:
            raise PackageError("'pipelines' must be a non-empty list")
        return [_parse_pipeline(item, manifest_path.name, i) for i, item in enumerate(items)]

    if isinstance(raw, dict):
        return [_parse_pipeline(raw, manifest_path.name, 0)]

    raise PackageError(f"{manifest_path.name} must contain a JSON object")


def render_airflow_dag(p: PipelineSpec) -> str:
    task_blocks = []
    ordered_task_ids = [t.task_id for t in p.tasks]
    for task in p.tasks:
        if task.operator != "docker":
            raise PackageError(
                f"Task '{task.task_id}' uses operator '{task.operator}'. "
                "Generated pipelines currently support only DockerOperator for container-level metrics."
            )
        if not task.script:
            raise PackageError(f"Task '{task.task_id}' is missing 'script'")
        task_blocks.append(_render_docker_task_block(p.dag_id, task))

    dependency_lines: list[str] = []
    explicit_dependencies = any(t.depends_on for t in p.tasks)
    if explicit_dependencies:
        task_ids = {t.task_id for t in p.tasks}
        for task in p.tasks:
            for upstream in task.depends_on:
                if upstream not in task_ids:
                    raise PackageError(
                        f"Task '{task.task_id}' depends on unknown task '{upstream}' in DAG '{p.dag_id}'"
                    )
                dependency_lines.append(f"    task_map[{upstream!r}] >> task_map[{task.task_id!r}]")
    else:
        for a, b in zip(ordered_task_ids, ordered_task_ids[1:]):
            dependency_lines.append(f"    task_map[{a!r}] >> task_map[{b!r}]")

    description_line = f"    description={p.description!r}," if p.description else ""
    return f'''from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DAG_ID = {p.dag_id!r}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule={p.schedule!r},
    catchup=False,
    tags={p.tags!r},
{description_line}
) as dag:
    task_map = {{}}

{chr(10).join(task_blocks)}

{chr(10).join(dependency_lines) if dependency_lines else "    pass"}
'''


def _parse_pipeline(raw: Any, source_name: str, index: int) -> PipelineSpec:
    if not isinstance(raw, dict):
        raise PackageError(f"Pipeline entry #{index} in {source_name} must be an object")

    dag_section = raw.get("dag") if isinstance(raw.get("dag"), dict) else {}
    dag_id = raw.get("dag_id") or dag_section.get("id") or dag_section.get("dag_id")
    _require_valid_id(dag_id, "dag_id")

    tasks_raw = raw.get("tasks")
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise PackageError(f"DAG '{dag_id}' must define a non-empty 'tasks' list")

    tasks = [_parse_task(t, dag_id=dag_id, index=i) for i, t in enumerate(tasks_raw)]
    _ensure_unique([t.task_id for t in tasks], f"Duplicate task_id in DAG '{dag_id}'")

    schedule = raw.get("schedule", dag_section.get("schedule"))
    if schedule is not None and not isinstance(schedule, str):
        raise PackageError(f"DAG '{dag_id}' schedule must be a string or null")

    tags = raw.get("tags", dag_section.get("tags", ["runner", "generated"]))
    if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
        raise PackageError(f"DAG '{dag_id}' tags must be a list of strings")

    description = raw.get("description", dag_section.get("description"))
    if description is not None and not isinstance(description, str):
        raise PackageError(f"DAG '{dag_id}' description must be a string")

    return PipelineSpec(
        dag_id=dag_id,
        tasks=tasks,
        schedule=schedule,
        tags=tags,
        description=description,
    )


def _parse_task(raw: Any, *, dag_id: str, index: int) -> TaskSpec:
    if not isinstance(raw, dict):
        raise PackageError(f"Task #{index} in DAG '{dag_id}' must be an object")

    task_id = raw.get("task_id") or raw.get("id")
    _require_valid_id(task_id, f"task_id (DAG '{dag_id}', task #{index})")

    operator = str(raw.get("operator", "docker")).lower()
    if operator not in {"docker", "python"}:
        raise PackageError(f"Task '{task_id}' in DAG '{dag_id}' uses unsupported operator '{operator}'")

    script = raw.get("script")
    if script is not None:
        if not isinstance(script, str):
            raise PackageError(f"Task '{task_id}' script must be a string")
        _validate_rel_path(script, field_name=f"script for task '{task_id}'")

    image = raw.get("image", DEFAULT_RUNNER_IMAGE)
    if not isinstance(image, str) or not image.strip():
        raise PackageError(f"Task '{task_id}' image must be a non-empty string")

    env = raw.get("env", {})
    if not isinstance(env, dict):
        raise PackageError(f"Task '{task_id}' env must be an object")
    normalized_env = {str(k): str(v) for k, v in env.items()}

    depends_on = raw.get("depends_on", [])
    if depends_on is None:
        depends_on = []
    if not isinstance(depends_on, list) or not all(isinstance(x, str) for x in depends_on):
        raise PackageError(f"Task '{task_id}' depends_on must be a list of task ids")

    metrics = raw.get("metrics", {})
    if metrics is None:
        metrics = {}
    if not isinstance(metrics, dict):
        raise PackageError(f"Task '{task_id}' metrics must be an object")

    metrics_enabled = bool(metrics.get("enabled", True))
    carbon_enabled = bool(metrics.get("carbon", True))

    return TaskSpec(
        task_id=task_id,
        operator=operator,
        script=script,
        image=image,
        env=normalized_env,
        depends_on=depends_on,
        metrics_enabled=metrics_enabled,
        carbon_enabled=carbon_enabled,
    )


def _render_docker_task_block(dag_id: str, task: TaskSpec) -> str:
    env = dict(task.env)
    env["SCRIPT"] = f"/app/scripts/{task.script}"
    env["DAG_ID"] = "{{ dag.dag_id }}"
    env["TASK_ID"] = "{{ task.task_id }}"
    env["CODECARBON_OUTPUT_DIR"] = env.get("CODECARBON_OUTPUT_DIR", "/app/data/emissions")
    env["CODECARBON_SAVE_TO_FILE"] = "true" if task.carbon_enabled else "false"
    if task.metrics_enabled:
        env["NODE_EXPORTER_TEXTFILE_DIR"] = env.get("NODE_EXPORTER_TEXTFILE_DIR", "/node_exporter")

    mounts = [
        'Mount(source="pipeline_scripts", target="/app/scripts", type="volume")',
        'Mount(source="pipeline_data", target="/app/data", type="volume")',
    ]
    if task.metrics_enabled:
        mounts.append('Mount(source="node_exporter_textfile", target="/node_exporter", type="volume")')

    mounts_src = ",\n            ".join(mounts)
    container_name = (
        "pes_"
        + _docker_safe_name(dag_id)
        + "_"
        + _docker_safe_name(task.task_id)
        + "_{{ ts_nodash }}_{{ ti.try_number }}"
    )
    return f'''    task_map[{task.task_id!r}] = DockerOperator(
        task_id={task.task_id!r},
        image={task.image!r},
        docker_url="unix://var/run/docker.sock",
        network_mode={DEFAULT_DOCKER_NETWORK!r},
        container_name={container_name!r},
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,
        environment={env!r},
        mounts=[
            {mounts_src},
        ],
    )'''


def _require_valid_id(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise PackageError(f"{field_name} must be a non-empty string")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", value):
        raise PackageError(
            f"{field_name} '{value}' is invalid. Use letters, numbers, underscore, dash, or dot."
        )


def _ensure_unique(values: list[str], message: str) -> None:
    if len(values) != len(set(values)):
        raise PackageError(message)


def _validate_rel_path(path_str: str, *, field_name: str) -> None:
    p = Path(path_str)
    if p.is_absolute():
        raise PackageError(f"{field_name} must be relative to the package scripts/ directory")
    if ".." in p.parts:
        raise PackageError(f"{field_name} may not contain '..'")


def _load_manifest_file(manifest_path: Path) -> Any:
    suffix = manifest_path.suffix.lower()
    text = manifest_path.read_text(encoding="utf-8")
    if suffix == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise PackageError(f"Invalid JSON in {manifest_path.name}: {e}") from e

    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as e:
            raise PackageError(
                f"YAML manifest '{manifest_path.name}' requires PyYAML to be installed in execution-api"
            ) from e
        try:
            return yaml.safe_load(text)
        except Exception as e:
            raise PackageError(f"Invalid YAML in {manifest_path.name}: {e}") from e

    raise PackageError(f"Unsupported manifest extension for {manifest_path.name}")


def _docker_safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-") or "task"
