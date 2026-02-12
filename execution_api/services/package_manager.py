from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from execution_api.utils.fs import (
    PackageError,
    chmod_recursive_readable,
    clear_dir,
    copy_tree,
    ensure_dir,
    safe_extract_zip,
)


EXPECTED_TOP_LEVEL = {"dags", "scripts", "data"}


@dataclass
class InstallResult:
    installed: dict[str, list[str]]  # folder -> list of top-level entries copied


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

            installed = {"dags": [], "scripts": [], "data": []}

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

            # permissions for Airflow + task-runner containers
            chmod_recursive_readable(self.dags_dir)
            chmod_recursive_readable(self.scripts_dir)
            chmod_recursive_readable(self.data_dir)

            return InstallResult(installed=installed)

    def _validate(self, extracted_dir: Path) -> None:
        # allow extra files like README/manifest, but require at least one expected folder
        present = {p.name for p in extracted_dir.iterdir() if p.is_dir()}
        if not (present & EXPECTED_TOP_LEVEL):
            raise PackageError(
                f"Zip must contain at least one of {sorted(EXPECTED_TOP_LEVEL)} at top-level. Found: {sorted(present)}"
            )

        # if user includes these folders, they must be directories
        for name in EXPECTED_TOP_LEVEL:
            p = extracted_dir / name
            if p.exists() and not p.is_dir():
                raise PackageError(f"'{name}' must be a directory in the zip")

    def _top_entries(self, p: Path) -> list[str]:
        return sorted([x.name for x in p.iterdir()])