"""
Entrypoint for pipeline-task-runner.

Reads SCRIPT env var and executes it, optionally wrapped in CodeCarbon tracking.

If NODE_EXPORTER_TEXTFILE_DIR is set (e.g. /node_exporter), emits Prometheus
textfile collector metrics so node-exporter can expose them to Prometheus.
"""

from __future__ import annotations

import os
import re
import runpy
import sys
import time
from pathlib import Path

SCRIPT = os.environ.get("SCRIPT")

CODECARBON_ENABLED = os.environ.get("CODECARBON_SAVE_TO_FILE", "false").lower() == "true"
CODECARBON_OUTPUT_DIR = os.environ.get("CODECARBON_OUTPUT_DIR", "/app/data/emissions")

TEXTFILE_DIR = os.environ.get("NODE_EXPORTER_TEXTFILE_DIR", "").strip()

# Prefer stable labels; avoid run_id cardinality explosion for MVP
DAG_ID = os.environ.get("DAG_ID", os.environ.get("AIRFLOW_CTX_DAG_ID", "unknown_dag"))
TASK_ID = os.environ.get("TASK_ID", os.environ.get("AIRFLOW_CTX_TASK_ID", "unknown_task"))


def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_:.-]+", "_", s)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def emit_textfile_metrics(duration_s: float, exit_code: int, emissions_kg: float) -> None:
    if not TEXTFILE_DIR:
        return

    labels = f'dag_id="{_safe(DAG_ID)}",task_id="{_safe(TASK_ID)}"'
    lines = [
        "# HELP pipeline_task_duration_seconds Task duration in seconds",
        "# TYPE pipeline_task_duration_seconds gauge",
        f"pipeline_task_duration_seconds{{{labels}}} {duration_s}",
        "# HELP pipeline_task_exit_code Task exit code (0=success)",
        "# TYPE pipeline_task_exit_code gauge",
        f"pipeline_task_exit_code{{{labels}}} {exit_code}",
        "# HELP pipeline_task_emissions_kg_co2 Estimated emissions for task run (kg CO2e)",
        "# TYPE pipeline_task_emissions_kg_co2 gauge",
        f"pipeline_task_emissions_kg_co2{{{labels}}} {emissions_kg}",
    ]
    fname = f"pipeline_{_safe(DAG_ID)}_{_safe(TASK_ID)}.prom"
    out = Path(TEXTFILE_DIR) / fname
    _atomic_write(out, "\n".join(lines) + "\n")
    print(f"Wrote textfile metrics: {out}")


def run_with_carbon(script_path: str) -> float:
    from codecarbon import EmissionsTracker

    os.makedirs(CODECARBON_OUTPUT_DIR, exist_ok=True)
    tracker = EmissionsTracker(
        output_dir=CODECARBON_OUTPUT_DIR,
        save_to_file=True,
        log_level="warning",
        project_name=f"{DAG_ID}.{TASK_ID}",
    )

    tracker.start()
    try:
        runpy.run_path(script_path, run_name="__main__")
        return 0.0  # placeholder, overwritten in finally
    finally:
        emissions = tracker.stop()
        try:
            return float(emissions) if emissions is not None else 0.0
        except Exception:
            return 0.0


def run_plain(script_path: str) -> None:
    runpy.run_path(script_path, run_name="__main__")


if __name__ == "__main__":
    if not SCRIPT:
        print("ERROR: SCRIPT environment variable is not set", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(SCRIPT):
        print(f"ERROR: Script not found: {SCRIPT}", file=sys.stderr)
        sys.exit(1)

    print(f"Running script: {SCRIPT}")
    print(f"CodeCarbon tracking: {CODECARBON_ENABLED}")
    if TEXTFILE_DIR:
        print(f"Prometheus textfile dir: {TEXTFILE_DIR}")
    else:
        print("Prometheus textfile dir: (disabled)")

    start = time.time()
    emissions_kg = 0.0
    exit_code = 0

    try:
        if CODECARBON_ENABLED:
            emissions_kg = run_with_carbon(SCRIPT)
        else:
            run_plain(SCRIPT)
    except Exception as e:
        exit_code = 1
        print(f"ERROR: Script raised exception: {e}", file=sys.stderr)
        raise
    finally:
        duration_s = time.time() - start
        emit_textfile_metrics(duration_s=duration_s, exit_code=exit_code, emissions_kg=emissions_kg)

    print("Script completed successfully.")