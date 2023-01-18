import json
import logging
import uuid
from pathlib import Path
from typing import Union

import pandas as pd
from tasks.helpers.common import generate_save_path, read_extracted_zone
from tasks.helpers.data_models import Metadata, TransformedZoneItem, TransformField
from tasks.helpers.tado_data_models import TadoDataModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def generate_days(
    metadata: Metadata, other: pd.DataFrame, days_path: str
) -> pd.DataFrame:
    days = other.assign(
        # day_id=UUID,
        extracted_path=str(metadata.base_path),
        transformed_path=str(days_path),
        date=metadata.date,
    )
    return days


def generate_climate(
    metadata: Metadata,
    humidity: pd.DataFrame,
    interior_temp: pd.DataFrame,
    climate_path: str,
) -> pd.DataFrame:
    climate = (
        pd.merge(left=humidity, right=interior_temp, on="timestamp", how="outer")
        .drop(columns=["value.fahrenheit"])
        .rename(
            columns={
                "_max": "humidity_max",
                "_min": "humidity_min",
                "_percentageUnit": "humidity_unit",
                "_valueType_x": "humidity_type",
                "value.celsius": "temperature",
                "_max.celsius": "temperature_max",
                "_min.celsius": "temperature_min",
                "_valueType_y": "temperature_type",
            }
        )
        .assign(
            temperature_unit="celsius",
            # day_id=UUID,
            extracted_path=str(metadata.base_path),
            transformed_path=str(climate_path),
            date=metadata.date,
        )
    )
    return climate


def generate_weather(
    metadata: Metadata,
    weather_condition: pd.DataFrame
) -> pd.DataFrame:
    weather = weather_condition
    return weather


def transform_one_zone(metadata: Metadata, i):
    hist_data = metadata.extract.historic_data[i]
    logger.info("BEGIN transform for zone_id: %s", hist_data.zone_id)
    tado_data = TadoDataModel.parse_file(hist_data.path)
    logger.debug("JSON data parsed to TadoDataModel")

    days_path = generate_save_path(
        metadata, hist_data.zone_id, ext=".csv", suffix="_days",
    )
    climate_path = generate_save_path(
        metadata, hist_data.zone_id, ext=".csv", suffix="_climate",
    )
    weather_path = generate_save_path(
        metadata, hist_data.zone_id, ext=".csv", suffix="_weather",
    )
    metadata.transform.paths.append(
        TransformedZoneItem(
            climate_path=climate_path, days_path=days_path, weather_path=weather_path
        )
    )

    # PARSE FROM TADO_DATA

    # UNUSED
    settings = pd.json_normalize(
        tado_data.settings.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    # FEED INTO DAYS TABLE
    other_fields = (
        pd.DataFrame(
            {
                k: [v]
                for k, v in tado_data.dict(by_alias=True).items()
                if k in ["hoursInDay", "zoneType", "interval"]
            }
        )
        .assign(
            from_=lambda x: x.interval.apply(lambda y: y["from"]),
            to=lambda x: x.interval.apply(lambda y: y["to"]),
        )
        .rename(columns={"from_": "from"})  # TODO: Redundant?
        .drop(columns=["interval"])
    )

    # FEED INTO CLIMATE TABLE
    humidity = pd.json_normalize(
        tado_data.measuredData.humidity.dict(by_alias=True),
        "dataPoints",
        ["max", "min", "percentageUnit", "valueType"],
        meta_prefix="_",
    )

    # FEED INTO CLIMATE TABLE
    interior_temp = pd.json_normalize(
        tado_data.measuredData.insideTemperature.dict(by_alias=True),
        "dataPoints",
        [["max", "celsius"], ["min", "celsius"], "valueType"],
        meta_prefix="_",
    )

    # UNUSED
    measuring_device_connected = pd.json_normalize(
        tado_data.measuredData.measuringDeviceConnected.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    # UNUSED
    # TODO: Maybe to get added in to weather metadata table?
    stripes = pd.json_normalize(
        tado_data.stripes.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    # TODO: NOT SURE ABOUT THIS
    call_for_heat = pd.json_normalize(
        tado_data.callForHeat.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
    )

    # EXPORT THIS FOR LOADING TOO
    weather_condition = pd.json_normalize(
        tado_data.weather.condition.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    # TODO: NOT SURE ABOUT THIS ONE
    weather_slots = (
        pd.DataFrame.from_dict(tado_data.weather.slots.slots, orient="index")
        .assign(
            temperature=lambda x: x["temperature"].apply(lambda y: y["celsius"])
        )
        .rename(columns={"temperature": "temperature_celsius"})
    )

    # UNUSED
    weather_sunny = pd.json_normalize(
        tado_data.weather.sunny.dict(by_alias=True),
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )
    logger.debug('Preprocessed basic tables, transform into days, climate and weather tables')
    # TRANSFORM STEP 1
    # CREATE 'days' rows
    days = generate_days(metadata, other_fields, days_path)
    days.to_csv(days_path, index=False)

    # TRANSFORM STEP 2
    # CREATE 'climate' rows
    climate = generate_climate(metadata, humidity, interior_temp, climate_path)
    climate.to_csv(climate_path, index=False)

    # TRANSFORM STEP 3
    # CREATE 'weather' rows
    weather = generate_weather(metadata, weather_condition)
    weather.to_csv(weather_path, index=False)

    logger.debug('days shape: %s', days.shape)
    logger.debug('climate shape: %s', climate.shape)
    logger.debug('weather shape: %s', weather.shape)

    return metadata


def transform(metadata: Metadata) -> Metadata:
    # TODO: Sort out data transform methods

    logger.info(f"Starting extract func")

    zone_data = read_extracted_zone(metadata)

    transformed_zone_items = []
    metadata.transform = TransformField(paths=[])
    for i, hist_data in enumerate(metadata.extract.historic_data):
        
        metadata = transform_one_zone(metadata, i)

        # TODO: EXTRACT FROM ZONE_DATA

        # transformed_zone_items.append(
        #     TransformedZoneItem(climate_path=climate_path, days_path=days_path)
        # )

    logger.debug("Updating metadata")
    metadata.transform = TransformField(paths=transformed_zone_items)
    return metadata
