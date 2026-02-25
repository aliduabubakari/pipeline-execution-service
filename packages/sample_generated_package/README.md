Sample declarative PES package.

Contents:
- `pipeline.yaml` (manifest-only pipeline spec)
- `scripts/transform.py`
- `data/input.csv`

This package intentionally omits `dags/` so the execution API generates an
instrumented Airflow DAG during upload.
