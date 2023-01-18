import json
import logging
import os
from pathlib import Path

from tasks.helpers.data_models import Metadata
from tasks.helpers.logs import make_logger
from tasks.validate import validate

logger = make_logger("root", add_handler=True, level="debug")


if __name__ == "__main__":
    logging.debug("Starting docker task for validate")

    xcom_pull = os.environ.get("XCOM_PULL")
    logging.debug("xcom_pull: %s", xcom_pull)

    # Parse json from xcom
    metadata = json.loads(xcom_pull)
    logging.debug("xcom JSON data: %s", metadata)

    metadata = Metadata(**metadata)
    logging.debug("xcom JSON parsed to: %s", metadata)

    metadata_new = validate(metadata=metadata)

    # Print to stdout for xcom push
    print(metadata_new.json())
