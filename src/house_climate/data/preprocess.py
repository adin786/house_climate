import logging
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(
    format="[%(asctime)s|%(levelname).4s|%(name)s] %(message)s",
    level='DEBUG',
)
logger = logging.getLogger('root')
CONNECTION_STRING = os.environ["DB_CONNECTION_STRING"]
DATA_DIR = Path.cwd() / "data/interim"
SAVE_PATH = DATA_DIR / "01_preprocessed.parquet"
START_DATE = "2022-01-01"
END_DATE = "2023-01-01"


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
    
    Numerical columns get linear interpolated from the existing 
    15minute intervals. Non numerics get forward filled.
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

    new_interior = (
        pd.merge(
            left=interpolated_num_cols,
            right=interpolated_other_cols,
            on='timestamp', 
            how='left'
        )
        .ffill()
        .astype({"zone_id": "Int8"})
    )
    return new_interior


def filter_by_time_ranges(df):
    """Merge both tables to get one unified table of 1-minute intervals for 
    temp, humidity and heating status
    """
    return df.loc[lambda x: (x.index >= x.t_start) & (x.index <= x.t_end)]


def prepare_interior_data(engine) -> DataFrame:
    logger.info('Extracting "interior" table from DB')
    interior_raw = pd.read_sql(
        dedent(f"""\
            SELECT *
            FROM interior
            WHERE extracted_date 
            BETWEEN '{START_DATE}' AND '{END_DATE}'"""
        ),
        con=engine,
    )
    logger.debug("shape: %s", interior_raw.shape)

    logger.info("Resampling to 1min intervals")
    interior = (
        interior_raw
        .groupby('zone_id').apply(clean_interior)
        .set_index('timestamp')
        .groupby('zone_id').apply(upsample_interior)
        .ffill()
        .reset_index(0, drop=True)
        .sort_values(by=['timestamp', 'zone_id'])
    )
    logger.debug("shape: %s", interior.shape)
    logger.debug("info: %s", interior.info())
    logger.debug('df:\n%s', interior)
    return interior


def prepare_heating_data(engine) -> DataFrame:
    logger.info("Extracting 'call_for_heat' table")
    heating_raw = pd.read_sql(
        dedent(f"""\
            SELECT *
            FROM call_for_heat
            WHERE extracted_date 
            BETWEEN '{START_DATE}' AND '{END_DATE}'"""
        ),
        con=engine,
    )
    logger.debug("shape: %s", heating_raw.shape)

    logger.info("Cleaning heat table")
    heating = (
        heating_raw
        .sort_values(by=['t_start', 'zone_id'])
        .astype({"zone_id": "Int8"})
    )
    logger.debug('shape: %s', heating.shape)
    logger.debug('info: %s', heating.info())
    logger.debug('df:\n%s', heating)
    return heating


def prepare_weather_data(engine) -> DataFrame:
    logger.info("Extracting 'weather' table")
    weather_raw = pd.read_sql(
        dedent(f"""\
            SELECT *
            FROM weather
            WHERE extracted_date 
            BETWEEN '{START_DATE}' AND '{END_DATE}'"""
        ),
        con=engine
    )
    logger.debug('shape: %s', weather_raw.shape)

    logger.info('Preprocessing weather table')
    weather = (
        weather_raw
        .sort_values(by=['t_start', 'zone_id'])
        .rename(columns={'temp_celsius': 'exterior_temp', 'state': 'weather'})
        .astype({"zone_id": "Int8"})
    )
    logger.debug('shape: %s', weather.shape)
    logger.debug('info: %s', weather.info())
    logger.debug('df:\n%s', weather)
    return weather


def prepare_days_data(engine) -> DataFrame:
    logger.info("Extracting 'days' table")
    days_raw = pd.read_sql(
        dedent(f"""\
            SELECT *
            FROM days
            WHERE extracted_date 
            BETWEEN '{START_DATE}' AND '{END_DATE}'"""
        ),
        con=ENGINE,
    )
    logger.debug('shape: %s', days_raw.shape)

    logger.info("Preprocessing days table")
    days = (
        days_raw
        .drop(columns=['historic_data', 'zone_metadata', 't_start', 't_end'])
        .sort_values(by=['extracted_date', 'zone_id'])
        .assign(extracted_datetime=lambda x: pd.to_datetime(x.extracted_date, utc=True))
        .drop(columns=['extracted_date'])
        .astype({"zone_id": "Int8"})
    )
    logger.debug('shape: %s', days.shape)
    logger.debug('info: %s', days.info())
    logger.debug('df:\n%s', days)
    return days


def merge_interior_heating(interior, heating) -> DataFrame:
    logger.info('Merging interior and heat tables')
    interior_heating = (
        pd.merge_asof(
            interior, heating, 
            by='zone_id', left_index=True, 
            right_on='t_start', 
            direction='backward'
        )
        .groupby('zone_id').apply(filter_by_time_ranges)
        .reset_index(0, drop=True)
        .sort_values(by=['timestamp', 'zone_id'])
        .loc[:, ['humidity', 'temperature', 'zone_id', 'value']]
    )
    logger.debug('shape: %s', interior_heating.shape)
    logger.debug('info: %s', interior_heating.info())
    logger.debug('df:\n%s', interior_heating)
    return interior_heating


def merge_interior_heating_weather(interior_heating, weather) -> DataFrame:
    logger.info("Merging interior, heat and weather tables")
    int_heat_weather = (
        pd.merge_asof(
            interior_heating, weather, 
            by='zone_id', 
            left_index=True, right_on='t_start', 
            direction='backward',
        )
        .groupby('zone_id').apply(filter_by_time_ranges)
        .reset_index(0, drop=True)
        .sort_values(by=['timestamp', 'zone_id'])
        .loc[:, ['humidity', 'temperature', 'zone_id', 'value', 'weather', 'exterior_temp', 'extracted_date']]
    )
    logger.debug('shape: %s', int_heat_weather.shape)
    logger.debug('info: %s', int_heat_weather.info())
    logger.debug('df:\n%s', int_heat_weather)
    return int_heat_weather


def merge_days(int_heat_weather, days) -> DataFrame:
    logger.info('Merging final table')
    merged = (
        pd.merge_asof(
            left=int_heat_weather, right=days,
            left_index=True, right_on='extracted_datetime', 
            direction='forward', by='zone_id'
        )
        .ffill()
        .drop(columns=['extracted_date', 'extracted_datetime'])
    )
    logger.debug('shape: %s', merged.shape)
    logger.debug('info: %s', merged.info())
    logger.debug('df:\n%s', merged)
    return merged


if __name__ == "__main__":
    logger.info('STARTING SCRIPT')

    logger.info('Create connection to DB')
    ENGINE = create_engine(CONNECTION_STRING)

    # Prepare tables
    interior = prepare_interior_data(ENGINE)
    heating = prepare_heating_data(ENGINE)
    weather = prepare_weather_data(ENGINE)
    days = prepare_days_data(ENGINE)

    # Join dataframes
    interior_heating = merge_interior_heating(interior, heating)
    int_heat_weather = merge_interior_heating_weather(interior_heating, weather)
    merged = merge_days(int_heat_weather, days)

    # Save to disk
    logger.info('Saving to %s', SAVE_PATH)
    merged.to_parquet(SAVE_PATH)
    logger.info("Saved: %.3f MB", SAVE_PATH.stat().st_size / 1024**2)

    logger.info('ENDING SCRIPT')

