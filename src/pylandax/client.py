import copy
import logging
import urllib

import requests

from .exceptions import LandaxAuthException


class Client:
    def __init__(
        self,
        url: str,
        version: str,
        username: str,
        password: str,
        client_id: str,
        client_secret: str,
    ):
        """
        Constructs a new pylandax client.
        :param url: Full URL of the Landax instance, e.g. https://example.landax.no
        :param version: API version to use, e.g. 'v32'
        :param username: Landax username
        :param password: Landax password
        :param client_id: OAuth client ID
        :param client_secret: OAuth client secret
        """

        self.logger = logging.getLogger(__name__)

        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret

        self.base_url = url.rstrip("/") + "/"
        self.api_url = f"{self.base_url}api/{version}/"
        self.headers = {}

        self.oauth_token = self._authenticate()
        self.headers["Authorization"] = "Bearer " + self.oauth_token

        self._typed_api = None
        self.version = version
        if version in _VERSIONED_CLIENTS:
            self._typed_api = _VERSIONED_CLIENTS[version](self)

    @property
    def api(self):
        """Typed API surface for the configured version."""
        if self._typed_api is None:
            raise AttributeError(
                f"No typed API available for version '{self.version}'. "
                f"Supported versions: {', '.join(_VERSIONED_CLIENTS)}."
            )
        return self._typed_api

    # -- HTTP verbs ----------------------------------------------------------

    def get(self, resource: str, entity_id: int, *, query_params: dict | None = None) -> dict | None:
        """GET a single entity by ID. Returns None on 404."""
        url = self._build_url(f"{self.api_url}{resource}({entity_id})", query_params)
        response = requests.get(url, headers=self.headers)
        if response.status_code == 404:
            return None
        return response.json()

    def list(self, resource: str, *, select: list[str] | None = None, query_params: dict | None = None) -> list[dict]:
        """GET all entities, automatically following pagination."""
        if query_params is None:
            query_params = {}

        if select is not None:
            query_params["select"] = ",".join(select)

        url = self._build_url(f"{self.api_url}{resource}", query_params)
        response = self._get(url)
        data = response.json()["value"]
        while "nextLink" in response.json():
            response = self._get(response.json()["nextLink"])
            data = data + response.json()["value"]

        return data

    def post(self, resource: str, *, data: dict) -> requests.Response:
        """POST a new entity."""
        url = self.api_url + resource
        headers = self._json_headers()
        return requests.post(url, json=data, headers=headers)

    def put(self, resource: str, entity_id: int, *, data: dict) -> requests.Response:
        """PUT (full replace) an entity by ID."""
        url = f"{self.api_url}{resource}({entity_id})"
        headers = self._json_headers()
        return requests.put(url, json=data, headers=headers)

    def patch(self, resource: str, entity_id: int, *, data: dict) -> requests.Response:
        """PATCH (partial update) an entity by ID."""
        url = f"{self.api_url}{resource}({entity_id})"
        headers = self._json_headers()
        return requests.patch(url, json=data, headers=headers)

    def delete(self, resource: str, entity_id: int) -> requests.Response:
        """DELETE an entity by ID."""
        url = f"{self.api_url}{resource}({entity_id})"
        return requests.delete(url, headers=self.headers)

    # -- Internal helpers ----------------------------------------------------

    def _get(self, url: str) -> requests.Response:
        """Authenticated GET to an absolute URL."""
        return requests.get(url, headers=self.headers)

    def _json_headers(self) -> dict:
        headers = copy.deepcopy(self.headers)
        headers["Content-Type"] = "application/json"
        return headers

    def _authenticate(self) -> str:
        url = self.base_url + "authenticate/token?grant_type=password"

        post_body = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        }

        result = requests.post(url, json=post_body)
        if result.status_code != 200:
            raise LandaxAuthException(
                "Landax returned non-200 response when getting OAuth token. Body: " + str(result.content)
            )

        response_data = result.json()

        if "access_token" not in response_data:
            raise LandaxAuthException("Landax response was non-json. Body: " + str(result.content))

        return response_data["access_token"]

    @staticmethod
    def _build_url(base_url: str, query_params: dict | None = None) -> str:
        if not query_params:
            return base_url
        return base_url + "?" + urllib.parse.urlencode(query_params)


def _load_v32(client: Client):
    from .v32 import ClientV32
    return ClientV32(client)


# Register typed API clients for each supported version here.
# To add v33: create src/pylandax/v33/, add a loader function, and register it.
_VERSIONED_CLIENTS: dict[str, callable] = {
    "v32": _load_v32,
}
