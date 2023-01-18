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


def transform(metadata: Metadata) -> Metadata:
    # TODO: Sort out data transform methods

    logger.info(f"Starting extract func")

    zone_data = read_extracted_zone(metadata)

    transformed_zone_items = []
    for i, hist_data in enumerate(metadata.extract.historic_data):
        # if i > 0:
        #     break
        logger.info("BEGIN transform for zone_id: %s", hist_data.zone_id)
        tado_data = TadoDataModel.parse_file(hist_data.path)
        logger.debug("JSON data parsed to TadoDataModel")

        days_path = generate_save_path(
            metadata.base_path,
            hist_data.zone_id,
            metadata.date,
            ext=".csv",
            suffix="_days",
        )
        climate_path = generate_save_path(
            metadata.base_path,
            hist_data.zone_id,
            metadata.date,
            ext=".csv",
            suffix="_climate",
        )

        # PARSE FROM TADO_DATA
        settings = pd.json_normalize(
            tado_data.settings.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
            meta_prefix="_",
        )
        other = (
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

        humidity = pd.json_normalize(
            tado_data.measuredData.humidity.dict(by_alias=True),
            "dataPoints",
            ["max", "min", "percentageUnit", "valueType"],
            meta_prefix="_",
        )
        interior_temp = pd.json_normalize(
            tado_data.measuredData.insideTemperature.dict(by_alias=True),
            "dataPoints",
            [["max", "celsius"], ["min", "celsius"], "valueType"],
            meta_prefix="_",
        )
        measuring_device_connected = pd.json_normalize(
            tado_data.measuredData.measuringDeviceConnected.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
            meta_prefix="_",
        )

        stripes = pd.json_normalize(
            tado_data.stripes.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
            meta_prefix="_",
        )
        call_for_heat = pd.json_normalize(
            tado_data.callForHeat.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
        )

        weather_condition = pd.json_normalize(
            tado_data.weather.condition.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
            meta_prefix="_",
        )
        weather_slots = (
            pd.DataFrame.from_dict(tado_data.weather.slots.slots, orient="index")
            .assign(
                temperature=lambda x: x["temperature"].apply(lambda y: y["celsius"])
            )
            .rename(columns={"temperature": "temperature_celsius"})
        )
        weather_sunny = pd.json_normalize(
            tado_data.weather.sunny.dict(by_alias=True),
            "dataIntervals",
            ["timeSeriesType", "valueType"],
            meta_prefix="_",
        )

        # TRANSFORM STEP 1
        # CREATE 'days' record
        days = generate_days(metadata, other, days_path)
        days.to_csv(days_path, index=False)

        # TRANSFORM STEP 2
        # CREATE 'climate' record
        climate = generate_climate(metadata, humidity, interior_temp, climate_path)
        climate.to_csv(climate_path, index=False)

        logger.debug(days)
        logger.debug(climate)

        # TODO: EXTRACT FROM ZONE_DATA

        transformed_zone_items.append(
            TransformedZoneItem(climate_path=climate_path, days_path=days_path)
        )

    logger.debug("Updating metadata")
    metadata.transform = TransformField(paths=transformed_zone_items)
    return metadata
