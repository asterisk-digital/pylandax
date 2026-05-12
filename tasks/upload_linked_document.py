import os
import io
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

    incident_id = 40

    image_filename = "testbilde2.jpg"
    with open(script_dir / image_filename, "rb") as file:
        # Read image to io.BytesIO object
        image_data = io.BytesIO(file.read())

    response = client.upload_linked_document(
        filedata=image_data,
        filename=image_filename,
        folder_id=None,
        module_name="INCIDENTS",
        linked_object_id=incident_id,
    )

    print(response)


if __name__ == "__main__":
    main()
