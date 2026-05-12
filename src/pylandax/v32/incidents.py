from __future__ import annotations

from typing import TYPE_CHECKING

from .models import Incident

if TYPE_CHECKING:
    from ..client import Client


class IncidentsAPI:
    """Typed wrapper around the Landax Incidents endpoints (v32)."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def get_all(self, query_params: dict | None = None, select: list[str] | None = None) -> list[Incident]:
        raw = self._client.list("Incidents", select=select, query_params=query_params)
        return [Incident.model_validate(item) for item in raw]

    def get(self, entity_id: int, query_params: dict | None = None) -> Incident | None:
        raw = self._client.get("Incidents", entity_id, query_params=query_params)
        if raw is None:
            return None
        return Incident.model_validate(raw)

    def create(self, incident: Incident) -> Incident:
        payload = incident.model_dump(by_alias=True, exclude_none=True)
        response = self._client.post("Incidents", data=payload)
        response.raise_for_status()
        return Incident.model_validate(response.json())

    def update(self, entity_id: int, incident: Incident) -> Incident | None:
        """Full replace of an Incident (HTTP PUT per the v32 spec)."""
        payload = incident.model_dump(by_alias=True, exclude_none=True)
        response = self._client.put("Incidents", entity_id, data=payload)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return Incident.model_validate(response.json())

    def delete(self, entity_id: int) -> None:
        response = self._client.delete("Incidents", entity_id)
        response.raise_for_status()
