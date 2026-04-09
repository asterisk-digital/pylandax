from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class CrudAPI[T: BaseModel](Protocol):
    """Protocol for a typed CRUD endpoint. Each versioned API module should conform to this."""

    def get_all(self, params: dict | None = None, select: list[str] | None = None) -> list[T]: ...
    def get(self, entity_id: int, params: dict | None = None) -> T | None: ...
    def create(self, entity: T) -> T: ...
    def update(self, entity_id: int, entity: T) -> T | None: ...
    def delete(self, entity_id: int) -> None: ...


@runtime_checkable
class VersionedClient(Protocol):
    """Protocol that every versioned client (ClientV32, ClientV33, ...) must satisfy."""

    incidents: CrudAPI
    documents: CrudAPI
