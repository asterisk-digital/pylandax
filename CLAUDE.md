# CLAUDE.md

## What is this?

pylandax is a Python client library for the Landax API. Multiple internal projects at Asterisk depend on it as a library (installed via git+ssh from GitHub).

## Project direction

The current codebase targets API v20 with untyped dict-based responses. The goal is a full rewrite targeting the **v32 spec** with Pydantic models for strong typing. The v32 work lives under `src/pylandax/v32/`. During the transition, the existing v20 interface must remain the default and stay backwards-compatible — nothing that imports pylandax today should break.

## Package layout

```
src/pylandax/
  client.py         # Core Client class (auth, base HTTP, version registry)
  versioned.py      # Protocols (CrudAPI, VersionedClient) for versioned API contract
  exceptions.py     # LandaxAuthException, LandaxDataException
  v32/
    models.py       # Pydantic models (Incident, Document, DTOs, enums)
    incidents.py    # Typed IncidentsAPI
    documents.py    # Typed DocumentsAPI (CRUD + content + linked uploads)
    client.py       # ClientV32 — aggregates typed endpoint classes
  _deprecated/
    client_v20.py   # Old v20 client with untyped document helpers (not importable)
    modules.json    # Legacy module name -> ID mapping
```

## Commands

- `uv sync --group dev` — install/sync dependencies including dev tools
- `uv run tox` — run tests (across py313, py314)
- `uv run ruff check .` — lint
- `uv run ruff format .` — format
- `uv run python <script>` — run any script

## OpenAPI spec

The files in `docs/` are **not committed to Git** (gitignored). If missing, download them:

```
curl -o docs/openapi_v32.json https://euroskilt.landax.no/api/v32/openapi.json
curl -o docs/openapi_v20.json https://euroskilt.landax.no/api/v20/openapi.json
```

## Key conventions

- Uses **uv** for package management. Never use pip.
- Python >=3.13.
- The OpenAPI spec for v32 is at `docs/openapi_v32.json`.

## Naming conventions

- **Pydantic model fields use PascalCase**, matching the Landax API spec exactly (e.g. `Id`, `Subject`, `IncidentDateTime`). No snake_case conversion. This keeps a 1:1 mapping with the API and avoids translation errors.
- **When a field name shadows its own type** (e.g. a field `DocumentType` of type `DocumentType`), suffix the Python field with `_` and use `Field(alias="DocumentType", serialization_alias="DocumentType")`. Access via `model.DocumentType_`, but serialization uses the API name. Set `model_config = {"populate_by_name": True}` on those models.
- **Never shadow Python built-ins** (`id`, `type`, `list`, etc.) in parameter names. Use descriptive names like `entity_id`, `type_id` instead.
- **Class and module names** follow the API spec: `Incident`, `IncidentsAPI`, `ClientV32`.
- **Method names** use snake_case per Python convention: `get_all`, `get`, `create`, `update`, `delete`.
- **Updates use HTTP PUT** (full replace), matching the v32 OpenAPI spec. The base Client provides both `put_data` and `patch_data`; v32 typed APIs use `put_data`.
