# CLAUDE.md

## What is this?

pylandax is a Python client library for the Landax API. Multiple internal projects at Asterisk depend on it as a library (installed via git+ssh from GitHub).

## Project direction

The current codebase targets API v20 with untyped dict-based responses. The goal is a full rewrite targeting the **v32 spec** with Pydantic models for strong typing. The v32 work lives under `src/pylandax/v32/`. During the transition, the existing v20 interface must remain the default and stay backwards-compatible — nothing that imports pylandax today should break.

## Package layout

```
src/pylandax/
  client.py         # Core Client class (v20 default, v32 shim via .v32 property)
  exceptions.py     # LandaxAuthException, LandaxDataException
  modules.json      # Module name -> ID mapping
  v32/
    models.py       # Pydantic models (Incident, etc.)
    incidents.py    # Typed IncidentsAPI
    client.py       # ClientV32 — aggregates typed endpoint classes
```

## Commands

- `uv sync` — install/sync dependencies
- `uv run python -m pytest` — run tests
- `uv run python <script>` — run any script

## OpenAPI spec

The file `docs/openapi_v32.json` is **not committed to Git** (gitignored). If it's missing, download it:

```
curl -o docs/openapi_v32.json https://euroskilt.landax.no/api/v32/openapi.json
```

## Key conventions

- Uses **uv** for package management. Never use pip.
- Python >=3.11.
- v32 models use Pydantic with `alias=PascalCase` (matching the API) and `snake_case` Python attributes.
- The OpenAPI spec for v32 is at `docs/openapi_v32.json`.
