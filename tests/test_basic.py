import json
from pathlib import Path

import pylandax

script_dir = Path(__file__).parent


def test_basic():
    confpath = Path(script_dir, "mock_config.json")
    with open(confpath) as file:
        conf = json.loads(file.read())["landax"]

    try:
        client = pylandax.Client(
            url=conf["url"],
            version=conf["version"],
            username=conf["username"],
            password=conf["password"],
            client_id=conf["client_id"],
            client_secret=conf["client_secret"],
        )
    # Since the URL is bogus, this is what we expect
    except pylandax.LandaxAuthException:
        pass


def test_generate_url():
    base_url = "https://test.landax.com"
    params = {"$test": "test", "$test2": "test2"}

    result = pylandax.Client.generate_url(base_url, params)
    assert result == "https://test.landax.com?%24test=test&%24test2=test2"

    result2 = pylandax.Client.generate_url(base_url, {})
    assert result2 == "https://test.landax.com"
