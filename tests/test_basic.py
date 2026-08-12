import os
import sys
import json
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

import pylandax

script_dir = Path(os.path.dirname(os.path.realpath(__file__)))


def test_basic():
    confpath = Path(script_dir, "mock_config.json")
    with open(confpath) as file:
        conf = json.loads(file.read())["landax"]

    # Mock the OAuth request so the test never hits the network. A non-200
    # response is what we expect the client to raise on.
    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.content = b'{"error": "unauthorized"}'

    with patch("pylandax.client.requests.post", return_value=mock_response):
        with pytest.raises(pylandax.LandaxAuthException):
            pylandax.Client(conf["url"], conf["credentials"])


def test_generate_url():
    base_url = "https://test.landax.com"
    params = {"$test": "test", "$test2": "test2"}

    result = pylandax.Client.generate_url(base_url, params)
    assert result == "https://test.landax.com?%24test=test&%24test2=test2"

    result2 = pylandax.Client.generate_url(base_url, {})
    assert result2 == "https://test.landax.com"
