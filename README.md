# pylandax

A Python client library for the Landax API. Under active development.

## Setup

```
uv sync --group dev
```

## Installation

Add to your project's `pyproject.toml` dependencies:

```
"pylandax @ git+ssh://git@github.com/asterisk-digital/pylandax.git@main"
```

## Usage (v32 typed API)

```python
import pylandax
from pylandax.v32.models import Incident, CreateDocumentWithLinkDto

client = pylandax.Client(
    url='https://eksempel.landax.no',
    version='v32',
    username='my_user',
    password='my_password',
    client_id='my_client_id',
    client_secret='my_client_secret',
)

# Incidents
incidents = client.api.incidents.get_all()
incident = client.api.incidents.get(42)
new_incident = client.api.incidents.create(Incident(Subject='Equipment failure'))
client.api.incidents.update(42, Incident(Subject='Updated subject', IsClosed=True))
client.api.incidents.delete(42)

# Documents
documents = client.api.documents.get_by_folder(100)
linked_docs = client.api.documents.get_linked('Incidents', 42)
content = client.api.documents.get_content(document_id=10, as_pdf=True)

# Upload a document linked to an incident
from io import BytesIO
filedata = BytesIO(open('report.pdf', 'rb').read())
result = client.api.documents.create_with_link(
    filedata=filedata,
    filename='report.pdf',
    document=CreateDocumentWithLinkDto(ModelName='Incidents', RecordId=42),
)
```

## Development

```
uv run ruff check .    # lint
uv run ruff format .   # format
uv run tox             # run tests (py313, py314)
```
