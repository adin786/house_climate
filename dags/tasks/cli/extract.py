from tasks.extract import extract
from tasks.helpers.logs import make_logger
from tasks.helpers.data_models import Metadata
import logging
from pathlib import Path
import os
import json


logger = make_logger('root', add_handler=True, level='debug')


if __name__ == "__main__":
    logging.debug('Starting docker task for extract')

    base_path = os.environ.get('BASE_PATH')
    logical_date = os.environ.get('LOGICAL_DATE')

    logging.debug('base_path: %s', base_path)
    logging.debug('logical_date: %s', logical_date)

    metadata = Metadata(
        base_path=base_path,
        date=logical_date,
    )
    logging.debug("Parsed Metadata: %s", metadata)

    metadata_new = extract(metadata)

    # Print to stdout for xcom push
    print(metadata_new.json())
