#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any


class TranslationError(Exception):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Translate a simple Airflow DockerOperator DAG into a PES pipeline manifest."
    )
    parser.add_argument("dag_file", help="Path to the Python DAG file")
    parser.add_argument(
        "-o",
        "--output",
        help="Output manifest path (default: <dag_stem>.pipeline.json next to DAG file)",
    )
    args = parser.parse_args()

    dag_path = Path(args.dag_file)
    if not dag_path.is_file():
        raise SystemExit(f"DAG file not found: {dag_path}")

    try:
        manifest = translate_dag_file(dag_path)
    except TranslationError as e:
        raise SystemExit(f"Translation failed: {e}") from e

    out_path = Path(args.output) if args.output else dag_path.with_name(f"{dag_path.stem}.pipeline.json")
    out_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote manifest: {out_path}")
    print(f"DAG ID: {manifest['dag_id']} ({len(manifest['tasks'])} tasks)")
    return 0


def translate_dag_file(path: Path) -> dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    consts = _collect_module_constants(tree)

    dag_id = _extract_dag_id(tree, consts)
    tags = _extract_dag_tags(tree, consts)

    tasks_by_var, task_order = _extract_docker_tasks(tree, consts)
    if not tasks_by_var:
        raise TranslationError("No DockerOperator task assignments found")

    edges = _extract_shift_edges(tree)
    depends_on_map: dict[str, list[str]] = {t["id"]: [] for t in tasks_by_var.values()}
    for upstream_var, downstream_var in edges:
        if upstream_var in tasks_by_var and downstream_var in tasks_by_var:
            upstream_id = tasks_by_var[upstream_var]["id"]
            downstream_id = tasks_by_var[downstream_var]["id"]
            depends_on_map[downstream_id].append(upstream_id)

    tasks = []
    for var_name in task_order:
        task = dict(tasks_by_var[var_name])
        upstream = depends_on_map.get(task["id"], [])
        if upstream:
            task["depends_on"] = _dedupe(upstream)
        tasks.append(task)

    manifest: dict[str, Any] = {"dag_id": dag_id, "tasks": tasks}
    if tags:
        manifest["tags"] = tags
    return manifest


def _collect_module_constants(tree: ast.Module) -> dict[str, Any]:
    consts: dict[str, Any] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name):
            try:
                consts[target.id] = ast.literal_eval(node.value)
            except Exception:
                continue
    return consts


def _extract_dag_id(tree: ast.Module, consts: dict[str, Any]) -> str:
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if isinstance(call, ast.Call) and _call_name(call.func) == "DAG":
                    for kw in call.keywords:
                        if kw.arg == "dag_id":
                            value = _eval_expr(kw.value, consts)
                            if isinstance(value, str) and value:
                                return value
    if isinstance(consts.get("DAG_ID"), str):
        return consts["DAG_ID"]
    raise TranslationError("Could not determine DAG ID")


def _extract_dag_tags(tree: ast.Module, consts: dict[str, Any]) -> list[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if isinstance(call, ast.Call) and _call_name(call.func) == "DAG":
                    for kw in call.keywords:
                        if kw.arg == "tags":
                            value = _eval_expr(kw.value, consts)
                            if isinstance(value, list) and all(isinstance(x, str) for x in value):
                                return value
    return []


def _extract_docker_tasks(tree: ast.Module, consts: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    tasks_by_var: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        if not isinstance(node.targets[0], ast.Name):
            continue
        var_name = node.targets[0].id
        if not isinstance(node.value, ast.Call):
            continue
        if _call_name(node.value.func) != "DockerOperator":
            continue

        kwargs = {kw.arg: kw.value for kw in node.value.keywords if kw.arg}
        task_id = _eval_expr(kwargs.get("task_id"), consts)
        if not isinstance(task_id, str) or not task_id:
            raise TranslationError(f"Task '{var_name}' is missing literal task_id")

        image = _eval_expr(kwargs.get("image"), consts)
        if image is not None and not isinstance(image, str):
            raise TranslationError(f"Task '{task_id}' has non-literal image")

        env = _eval_expr(kwargs.get("environment"), consts) or {}
        if not isinstance(env, dict):
            raise TranslationError(f"Task '{task_id}' has non-literal environment")
        env = {str(k): str(v) for k, v in env.items()}

        script = _normalize_script_from_env(env.get("SCRIPT"))
        if not script:
            raise TranslationError(
                f"Task '{task_id}' environment is missing SCRIPT=/app/scripts/... (required for translation)"
            )

        filtered_env = {
            k: v
            for k, v in env.items()
            if k
            not in {
                "SCRIPT",
                "DAG_ID",
                "TASK_ID",
                "NODE_EXPORTER_TEXTFILE_DIR",
                "CODECARBON_OUTPUT_DIR",
                "CODECARBON_SAVE_TO_FILE",
            }
        }

        task: dict[str, Any] = {"id": task_id, "script": script}
        if image and image != "pipeline-task-runner:latest":
            task["image"] = image
        if filtered_env:
            task["env"] = filtered_env

        tasks_by_var[var_name] = task
        order.append(var_name)

    return tasks_by_var, order


def _normalize_script_from_env(script_value: str | None) -> str | None:
    if not script_value or not isinstance(script_value, str):
        return None
    prefix = "/app/scripts/"
    if script_value.startswith(prefix):
        return script_value[len(prefix):]
    return None


def _extract_shift_edges(tree: ast.Module) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Expr):
            continue
        chain = _flatten_shift_chain(node.value, ast.RShift)
        if len(chain) >= 2:
            edges.extend((chain[i], chain[i + 1]) for i in range(len(chain) - 1))
    return edges


def _flatten_shift_chain(node: ast.AST, op_type: type[ast.operator]) -> list[str]:
    if not isinstance(node, ast.BinOp) or not isinstance(node.op, op_type):
        name = _node_name(node)
        return [name] if name else []

    left = _flatten_shift_chain(node.left, op_type)
    right = _flatten_shift_chain(node.right, op_type)
    return left + right


def _node_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return None


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _eval_expr(node: ast.AST | None, consts: dict[str, Any]) -> Any:
    if node is None:
        return None
    if isinstance(node, ast.Name) and node.id in consts:
        return consts[node.id]
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            out.append(item)
            seen.add(item)
    return out


if __name__ == "__main__":
    raise SystemExit(main())
