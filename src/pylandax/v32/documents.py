from __future__ import annotations

import io
import json
from io import BytesIO
from typing import TYPE_CHECKING

import requests as req

from ..exceptions import LandaxDataException
from .models import CreateDocumentDto, CreateDocumentWithLinkDto, CreatedDocumentDto, CreatedDocumentWithLinkDto, Document

if TYPE_CHECKING:
    from ..client import Client


class DocumentsAPI:
    """Typed wrapper around the Landax Documents endpoints (v32)."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def get_all(self, query_params: dict | None = None, select: list[str] | None = None) -> list[Document]:
        raw = self._client.list("Documents", select=select, query_params=query_params)
        return [Document.model_validate(item) for item in raw]

    def get(self, entity_id: int, query_params: dict | None = None) -> Document | None:
        raw = self._client.get("Documents", entity_id, query_params=query_params)
        if raw is None:
            return None
        return Document.model_validate(raw)

    def get_by_folder(self, folder_id: int) -> list[Document]:
        return self.get_all(query_params={"filter": f"FolderId eq {folder_id}"})

    def get_linked(self, model: str, record_id: int) -> list[Document]:
        """Get documents linked to a model record, e.g. get_linked('Incidents', 42)."""
        raw = self._client.list(f"{model}({record_id})/Documents")
        return [Document.model_validate(item) for item in raw]

    def get_content(self, document_id: int, as_pdf: bool = False) -> BytesIO | None:
        """Download document file content. Returns None if still processing (202)."""
        original = "False" if as_pdf else "True"
        url = (
            self._client.api_url
            + f"Documents/GetContent?documentid={document_id}&original={original}&encode=raw"
        )
        response = self._client._get(url)

        if response.status_code == 202:
            return None
        if response.status_code != 200:
            raise LandaxDataException(
                f"Error in GET {url}. Status: {response.status_code}. Body: {response.text}"
            )
        return BytesIO(response.content)

    def push_content(self, document_id: int, data: io.BytesIO) -> req.Response:
        """Replace file content of an existing document."""
        url = self._client.api_url + f"Documents/PushContent?documentid={document_id}"
        response = req.post(url, data=data.read(), headers=self._client.headers)
        response.raise_for_status()
        return response

    def create(
        self,
        filedata: io.BytesIO,
        filename: str,
        document: CreateDocumentDto,
    ) -> CreatedDocumentDto:
        """Upload a new document via Documents/CreateDocument (multipart)."""
        files = {
            "document": (None, json.dumps(document.model_dump(by_alias=True, exclude_none=True))),
            "fileData": (filename, filedata),
        }
        url = self._client.api_url + "Documents/CreateDocument"
        response = req.post(url, files=files, headers=self._client.headers)
        response.raise_for_status()
        return CreatedDocumentDto.model_validate(response.json()["value"])

    def create_with_link(
        self,
        filedata: io.BytesIO,
        filename: str,
        document: CreateDocumentWithLinkDto,
    ) -> CreatedDocumentWithLinkDto:
        """Upload a document linked to a model record via Documents/CreateDocumentWithLink."""
        files = {
            "document": (None, json.dumps(document.model_dump(by_alias=True, exclude_none=True))),
            "fileData": (filename, filedata),
        }
        url = self._client.api_url + "Documents/CreateDocumentWithLink"
        response = req.post(url, files=files, headers=self._client.headers)
        response.raise_for_status()
        return CreatedDocumentWithLinkDto.model_validate(response.json()["value"])

    def update(self, entity_id: int, document: Document) -> Document | None:
        """Full replace of a Document (HTTP PUT)."""
        payload = document.model_dump(by_alias=True, exclude_none=True)
        response = self._client.put("Documents", entity_id, data=payload)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return Document.model_validate(response.json())

    def delete(self, entity_id: int) -> None:
        response = self._client.delete("Documents", entity_id)
        response.raise_for_status()
