#!/usr/bin/env bash

set -e

echo "Creating project structure..."

touch README.md
touch Makefile
touch .env.example
touch .gitignore

# -----------------------
# compose
# -----------------------
mkdir -p compose/profiles

touch compose/docker-compose.core.yml
touch compose/docker-compose.backends.yml
touch compose/docker-compose.monitoring.yml
touch compose/docker-compose.override.yml
touch compose/profiles/dev.env

# -----------------------
# airflow
# -----------------------
mkdir -p airflow/dags
mkdir -p airflow/plugins

touch airflow/Dockerfile
touch airflow/requirements.txt
touch airflow/airflow.cfg

touch airflow/dags/__init__.py
touch airflow/dags/example_runner_dag.py

touch airflow/plugins/__init__.py

# -----------------------
# runner
# -----------------------
mkdir -p runner
touch runner/Dockerfile
touch runner/run_with_carbon.py

# -----------------------
# execution_api
# -----------------------
mkdir -p execution_api/services
mkdir -p execution_api/utils

touch execution_api/Dockerfile
touch execution_api/app.py
touch execution_api/requirements.txt
touch execution_api/schemas.py

touch execution_api/services/__init__.py
touch execution_api/services/airflow_client.py
touch execution_api/services/package_manager.py

touch execution_api/utils/__init__.py
touch execution_api/utils/fs.py

# -----------------------
# monitoring
# -----------------------
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/grafana/provisioning/dashboards
mkdir -p monitoring/grafana/dashboards

touch monitoring/prometheus.yml
touch monitoring/grafana/provisioning/datasources/datasource.yml
touch monitoring/grafana/provisioning/dashboards/dashboards.yml

touch monitoring/grafana/dashboards/airflow.json
touch monitoring/grafana/dashboards/containers.json
touch monitoring/grafana/dashboards/carbon.json

# -----------------------
# packages (samples)
# -----------------------
mkdir -p packages/sample_package/dags
mkdir -p packages/sample_package/scripts
mkdir -p packages/sample_package/data

touch packages/sample_package/README.md

echo "✅ Project scaffold created successfully!"