from __future__ import annotations

from typing import Any


class DockerEngineClient:
    def __init__(self, base_url: str | None = None):
        self.base_url = base_url

    def list_containers(self) -> list[dict[str, Any]]:
        import docker  # imported lazily so API still starts if dependency/socket unavailable

        client = docker.from_env() if self.base_url is None else docker.DockerClient(base_url=self.base_url)
        containers = client.containers.list(all=True)
        rows: list[dict[str, Any]] = []
        for c in containers:
            attrs = c.attrs or {}
            labels = attrs.get("Config", {}).get("Labels", {}) or {}
            rows.append(
                {
                    "id": c.id,
                    "name": c.name,
                    "status": getattr(c, "status", None),
                    "image": attrs.get("Config", {}).get("Image"),
                    "compose_project": labels.get("com.docker.compose.project"),
                    "compose_service": labels.get("com.docker.compose.service"),
                    "labels": labels,
                }
            )
        return rows
