from tasks.transform import transform
from tasks.helpers.logs import make_logger
import logging
from pathlib import Path
import os
import json
from tasks.helpers.data_models import Metadata


logger = make_logger('root', add_handler=True, level='debug')


if __name__ == "__main__":
    logging.debug('Starting docker task for transform')

    xcom_pull = os.environ.get('XCOM_PULL')
    logging.debug('xcom_pull: %s', xcom_pull)
    
    # Parse json from xcom
    metadata = json.loads(xcom_pull)
    logging.debug('xcom JSON data: %s', metadata)

    metadata = Metadata(**metadata)
    logging.debug('xcom JSON parsed to: %s', metadata)

    metadata_new = transform(metadata=metadata)

    # Print to stdout for xcom push
    print(metadata_new.json())
