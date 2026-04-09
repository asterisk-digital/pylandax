from __future__ import annotations

from typing import TYPE_CHECKING

from .incidents import IncidentsAPI

if TYPE_CHECKING:
    from ..client import Client


class ClientV32:
    """Typed v32 API surface. Delegates HTTP calls to the base Client."""

    def __init__(self, client: Client) -> None:
        self._client = client
        self.incidents = IncidentsAPI(client)
