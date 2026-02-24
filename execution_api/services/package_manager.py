from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from execution_api.services.pipeline_spec import load_pipeline_specs, render_airflow_dag
from execution_api.utils.fs import (
    PackageError,
    chmod_recursive_readable,
    clear_dir,
    copy_tree,
    ensure_dir,
    safe_extract_zip,
)


EXPECTED_TOP_LEVEL = {"dags", "scripts", "data"}
PIPELINE_MANIFEST_FILENAMES = (
    "pipeline.json",
    "pipeline-spec.json",
    "pipeline.yaml",
    "pipeline.yml",
    "pipeline-spec.yaml",
    "pipeline-spec.yml",
)


@dataclass
class InstallResult:
    installed: dict[str, list[str]]  # folder -> list of top-level entries copied
    generated_dags: list[str]
    package_mode: str


class PackageManager:
    def __init__(self, dags_dir: str, scripts_dir: str, data_dir: str):
        self.dags_dir = Path(dags_dir)
        self.scripts_dir = Path(scripts_dir)
        self.data_dir = Path(data_dir)

        ensure_dir(self.dags_dir)
        ensure_dir(self.scripts_dir)
        ensure_dir(self.data_dir)

    def install_zip(self, zip_bytes: bytes, *, replace: bool = True) -> InstallResult:
        """
        Installs:
          package:dags/*    -> DAGS_DIR/*
          package:scripts/* -> SCRIPTS_DIR/*
          package:data/*    -> DATA_DIR/*
        """
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            zip_path = td_path / "package.zip"
            zip_path.write_bytes(zip_bytes)

            extracted = td_path / "extracted"
            safe_extract_zip(zip_path, extracted)

            self._validate(extracted)

            if replace:
                clear_dir(self.dags_dir)
                clear_dir(self.scripts_dir)
                clear_dir(self.data_dir)

            installed = {"dags": [], "scripts": [], "data": [], "manifests": []}

            dags_src = extracted / "dags"
            if dags_src.exists():
                installed["dags"] = self._top_entries(dags_src)
                copy_tree(dags_src, self.dags_dir)

            scripts_src = extracted / "scripts"
            if scripts_src.exists():
                installed["scripts"] = self._top_entries(scripts_src)
                copy_tree(scripts_src, self.scripts_dir)

            data_src = extracted / "data"
            if data_src.exists():
                installed["data"] = self._top_entries(data_src)
                copy_tree(data_src, self.data_dir)

            manifest_files = self._find_manifest_files(extracted)
            generated_dags: list[str] = []
            for manifest in manifest_files:
                installed["manifests"].append(str(manifest.relative_to(extracted)))
                for spec in load_pipeline_specs(manifest):
                    dag_filename = f"{spec.dag_id}.generated.py"
                    dag_path = self.dags_dir / dag_filename
                    dag_path.write_text(render_airflow_dag(spec), encoding="utf-8")
                    generated_dags.append(spec.dag_id)
                    if dag_filename not in installed["dags"]:
                        installed["dags"].append(dag_filename)

            installed["dags"] = sorted(installed["dags"])
            installed["manifests"] = sorted(installed["manifests"])

            # permissions for Airflow + task-runner containers
            chmod_recursive_readable(self.dags_dir)
            chmod_recursive_readable(self.scripts_dir)
            chmod_recursive_readable(self.data_dir)

            package_mode = "generated" if generated_dags and not dags_src.exists() else "mixed" if generated_dags else "legacy"
            return InstallResult(
                installed=installed,
                generated_dags=sorted(generated_dags),
                package_mode=package_mode,
            )

    def _validate(self, extracted_dir: Path) -> None:
        # allow extra files like README/manifest, but require a recognized pipeline payload
        present = {p.name for p in extracted_dir.iterdir() if p.is_dir()}
        has_manifest = bool(self._find_manifest_files(extracted_dir))
        if not (present & EXPECTED_TOP_LEVEL) and not has_manifest:
            raise PackageError(
                "Zip must contain at least one of "
                f"{sorted(EXPECTED_TOP_LEVEL)} at top-level or a supported pipeline manifest "
                f"({list(PIPELINE_MANIFEST_FILENAMES)} / pipelines/*.json). Found dirs: {sorted(present)}"
            )

        # if user includes these folders, they must be directories
        for name in EXPECTED_TOP_LEVEL:
            p = extracted_dir / name
            if p.exists() and not p.is_dir():
                raise PackageError(f"'{name}' must be a directory in the zip")

    def _top_entries(self, p: Path) -> list[str]:
        return sorted([x.name for x in p.iterdir()])

    def _find_manifest_files(self, extracted_dir: Path) -> list[Path]:
        manifests: list[Path] = []
        for filename in PIPELINE_MANIFEST_FILENAMES:
            p = extracted_dir / filename
            if p.exists():
                if not p.is_file():
                    raise PackageError(f"'{filename}' must be a file in the zip")
                manifests.append(p)

        pipelines_dir = extracted_dir / "pipelines"
        if pipelines_dir.exists():
            if not pipelines_dir.is_dir():
                raise PackageError("'pipelines' must be a directory in the zip")
            for pattern in ("*.json", "*.yaml", "*.yml"):
                manifests.extend(sorted(pipelines_dir.glob(pattern)))

        return sorted(set(manifests), key=lambda p: str(p))
