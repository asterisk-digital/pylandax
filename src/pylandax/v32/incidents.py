from __future__ import annotations

from typing import TYPE_CHECKING

from .models import Incident

if TYPE_CHECKING:
    from ..client import Client


class IncidentsAPI:
    """Typed wrapper around the Landax Incidents endpoints (v32)."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def get_all(self, params: dict | None = None, select: list[str] | None = None) -> list[Incident]:
        raw = self._client.get_all_data("Incidents", params=params, select=select)
        return [Incident.model_validate(item) for item in raw]

    def get(self, incident_id: int, params: dict | None = None) -> Incident | None:
        raw = self._client.get_single_data("Incidents", incident_id, params=params)
        if raw is None:
            return None
        return Incident.model_validate(raw)

    def create(self, incident: Incident) -> Incident:
        payload = incident.model_dump(by_alias=True, exclude_none=True)
        response = self._client.post_data("Incidents", payload)
        response.raise_for_status()
        return Incident.model_validate(response.json())

    def update(self, incident_id: int, incident: Incident) -> Incident | None:
        payload = incident.model_dump(by_alias=True, exclude_none=True)
        response = self._client.patch_data("Incidents", incident_id, payload)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return Incident.model_validate(response.json())

    def delete(self, incident_id: int) -> None:
        response = self._client.delete_data("Incidents", str(incident_id))
        response.raise_for_status()
