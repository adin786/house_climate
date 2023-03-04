import os
import time
from pathlib import Path

from tasks.extract import extract
from tasks.helpers.data_models import Metadata
from tasks.helpers.logs import make_logger

logger = make_logger("root", add_handler=True, level="debug")


if __name__ == "__main__":
    logger.debug("Starting docker task for extract")

    base_path = os.environ.get("BASE_PATH")
    metadata_path = Path(base_path) / "metadata.json"
    logical_date = os.environ.get("LOGICAL_DATE")

    logger.debug("base_path: %s", base_path)
    logger.debug("logical_date: %s", logical_date)

    metadata = Metadata(
        base_path=base_path,
        date=logical_date,
    )
    logger.debug("Parsed Metadata: %s", metadata)

    metadata = extract(metadata)
    logger.debug("output metadata: %s", metadata.json())

    metadata_path.write_text(metadata.json(sort_keys=True, indent=4), encoding="utf-8")

    # Print to stdout for xcom push
    time.sleep(0.1)
    logger.info(str(metadata_path))
