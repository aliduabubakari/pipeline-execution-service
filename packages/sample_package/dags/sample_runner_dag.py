from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DAG_ID = "sample_runner_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mvp", "runner"],
) as dag:

    run_transform = DockerOperator(
        task_id="run_transform",
        image="pipeline-task-runner:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,
        environment={
            "SCRIPT": "/app/scripts/transform.py",
            "INPUT_FILE": "/app/data/input.csv",
            "OUTPUT_FILE": "/app/data/output.csv",

            # Make runner emit Prometheus textfile metrics
            "NODE_EXPORTER_TEXTFILE_DIR": "/node_exporter",

            # Provide stable labels (don’t rely on AIRFLOW_CTX_* availability)
            "DAG_ID": "{{ dag.dag_id }}",
            "TASK_ID": "{{ task.task_id }}",

            # CodeCarbon
            "CODECARBON_OUTPUT_DIR": "/app/data/emissions",
            "CODECARBON_SAVE_TO_FILE": "true",
        },
        mounts=[
            Mount(source="pipeline_scripts", target="/app/scripts", type="volume"),
            Mount(source="pipeline_data", target="/app/data", type="volume"),

            # This is the missing piece
            Mount(source="node_exporter_textfile", target="/node_exporter", type="volume"),
        ],
    )