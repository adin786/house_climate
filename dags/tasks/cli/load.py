from tasks.load import load
from tasks.helpers.logs import make_logger
import logging
from pathlib import Path
import os
import json


logger = make_logger('root', add_handler=True, level='debug')


if __name__ == "__main__":
    logging.debug('Starting docker task for load')

    xcom_pull = os.environ.get('XCOM_PULL')
    logging.debug('xcom_pull: %s', xcom_pull)
    
    # Parse json from xcom
    metadata = json.loads(xcom_pull)
    logging.debug('xcom JSON data: %s', metadata)

    metadata_new = load(metadata=metadata)

    # Print to stdout for xcom push
    print(json.dumps(metadata_new))
