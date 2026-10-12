import os
import sys
from pathlib import Path

import dotenv

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

import pylandax

script_dir = Path(os.path.dirname(os.path.realpath(__file__)))


def main():
    envpath = Path(script_dir, "..", ".env-dev")
    dotenv.load_dotenv(envpath)

    client = pylandax.Client(
        url=os.getenv("LANDAX_URL"),
        version="v20",
        username=os.getenv("LANDAX_USERNAME"),
        password=os.getenv("LANDAX_PASSWORD"),
        client_id=os.getenv("LANDAX_CLIENT_ID"),
        client_secret=os.getenv("LANDAX_CLIENT_SECRET"),
    )

    linked_documents = client.get_linked_documents("INCIDENTS", 36)

    print(linked_documents)


if __name__ == "__main__":
    main()
