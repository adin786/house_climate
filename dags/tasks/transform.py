from pathlib import Path
import logging
import pandas as pd



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def transform(tado_data):
    logger.info(f'Starting extract func')
    logger.debug(tado_data.keys())

    keys = tado_data.keys()

    # summary_stats = tado_data[['max', 'min', 'percentageUnit']]
    # [k for k in keys if k != 'measuredData']

    logger.debug(pd.DataFrame.from_records(tado_data["measuredData"]["insideTemperature"]["dataPoints"]))
    return tado_data