import json
import os
import time
from pathlib import Path

from tasks.helpers.data_models import Metadata
from tasks.helpers.logs import make_logger
from tasks.validate import validate

logger = make_logger("root", add_handler=True, level="debug")


if __name__ == "__main__":
    logger.debug("Starting docker task for validate")

    xcom_pull = os.environ.get("XCOM_PULL")
    logger.debug("xcom_pull: %s", xcom_pull)

    # Parse json from xcom
    metadata_path = Path(xcom_pull)
    metadata_text = metadata_path.read_text(encoding="utf-8")
    metadata = json.loads(metadata_text)
    logger.debug("xcom JSON data: %s", metadata)

    metadata = Metadata(**metadata)
    logger.debug("xcom JSON parsed to: %s", metadata)

    metadata_new = validate(metadata)
    logger.debug("output metadata: %s", metadata.json())

    metadata_path.write_text(metadata.json(sort_keys=True, indent=4), encoding="utf-8")

    # Print to stdout for xcom push
    time.sleep(0.1)
    logger.info(str(metadata_path))
