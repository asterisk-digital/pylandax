import json
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

import pylandax

script_dir = Path(__file__).parent


def test_basic():
    confpath = Path(script_dir, "mock_config.json")
    with open(confpath) as file:
        conf = json.loads(file.read())["landax"]

    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.content = b'{"error": "unauthorized"}'

    with patch("pylandax.client.requests.post", return_value=mock_response):
        with pytest.raises(pylandax.LandaxAuthException):
            pylandax.Client(
                url=conf["url"],
                version=conf["version"],
                username=conf["username"],
                password=conf["password"],
                client_id=conf["client_id"],
                client_secret=conf["client_secret"],
            )


def test_build_url():
    base_url = "https://test.landax.com"
    params = {"$test": "test", "$test2": "test2"}

    result = pylandax.Client._build_url(base_url, params)
    assert result == "https://test.landax.com?%24test=test&%24test2=test2"

    result2 = pylandax.Client._build_url(base_url, {})
    assert result2 == "https://test.landax.com"
