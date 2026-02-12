from __future__ import annotations

import os
import shutil
import stat
import zipfile
from pathlib import Path


class PackageError(Exception):
    pass


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def clear_dir(path: str | Path) -> None:
    p = Path(path)
    if not p.exists():
        return
    for child in p.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def _is_within_directory(base_dir: Path, target: Path) -> bool:
    base = base_dir.resolve()
    tgt = target.resolve()
    return str(tgt).startswith(str(base) + os.sep) or tgt == base


def safe_extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """
    Prevent Zip Slip: disallow entries that escape dest_dir.
    """
    ensure_dir(dest_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = dest_dir / member.filename
            if not _is_within_directory(dest_dir, member_path):
                raise PackageError(f"Unsafe zip entry: {member.filename}")
        zf.extractall(dest_dir)


def copy_tree(src: Path, dst: Path) -> None:
    """
    Copy directory content into dst (dst may exist).
    """
    ensure_dir(dst)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def chmod_recursive_readable(path: Path) -> None:
    """
    Make installed content readable by other containers/users (Airflow runs as non-root).
    """
    if not path.exists():
        return

    for root, dirs, files in os.walk(path):
        root_p = Path(root)
        for d in dirs:
            p = root_p / d
            mode = p.stat().st_mode
            p.chmod(mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        for f in files:
            p = root_p / f
            mode = p.stat().st_mode
            p.chmod(mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)