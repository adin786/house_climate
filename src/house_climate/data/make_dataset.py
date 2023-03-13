import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
import seaborn as sns
from pathlib import Path
from dotenv import load_dotenv
import logging
import os


load_dotenv()
logging.basicConfig(
    format="[%(asctime)s|%(levelname).4s|%(name)s] %(message)s",
    level='DEBUG',
)
logger = logging.getLogger('root')
CONNECTION_STRING = os.environ["DB_CONNECTION_STRING"]


# Clean up duplicates
def clean_interior(interior: DataFrame) -> DataFrame:
    """Clean up duplicates"""
    interior = (
        interior
        .drop_duplicates('timestamp', keep='first')
    )
    return interior


def upsample_interior(interior: DataFrame) -> DataFrame:
    """Resample timeseries to 1minute intervals
    
    Numerical columns get linear interpolated from the existing 15minute intervals
    Non numerics get forward filled
    """
    interpolated_num_cols = (
        interior
        .loc[:, ["humidity", "temperature"]]
        .resample('1T')
        .interpolate('linear')
    )

    interpolated_other_cols = (
        interior
        .loc[:, ["zone_id", "humidity_unit", "temperature_unit"]]
    )

    interior = (
        pd.merge(left=interpolated_num_cols, right=interpolated_other_cols, on='timestamp', how='left')
        .ffill()
        .astype({"zone_id": "int"})
    )
    return interior


if __name__ == "__main__":
    logger.info('STARTING SCRIPT')

    logger.info('Create connection to DB')
    engine = create_engine(CONNECTION_STRING)

    logger.info('Extracting "interior" table from DB')
    with engine.begin() as conn:
        interior = pd.read_sql("interior", con=engine)
    logger.debug("shape: %s", interior.shape)

    logger.info("Clean interior table")
    cleaned_ = (
        interior
        .groupby('zone_id').apply(clean_interior)
        .set_index('timestamp')
    )
    logger.debug("shape: %s", cleaned_.shape)

    logger.info("Resampling to 1min intervals")
    interior_resampled = (
        interior
        .groupby('zone_id').apply(clean_interior)
        .set_index('timestamp')
        .groupby('zone_id').apply(upsample_interior)
        .reset_index(0, drop=True)
        .sort_values(by=['timestamp', 'zone_id'])
    )
    logger.debug("shape: %s", interior_resampled.shape)

    logger.info("Extracting 'call_for_heat' table")
    with engine.begin() as conn:
        heat = pd.read_sql("call_for_heat", con=engine)

    logger.debug("shape: %s", heat.shape)








    logger.info('ENDING SCRIPT')

