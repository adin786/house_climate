import logging
import uuid
from pathlib import Path
from typing import Union

import pandas as pd

from tasks.common import generate_save_path
from tasks.common import read_extracted_historic, read_extracted_zone


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
UUID = uuid.uuid4().hex


def transform(metadata: dict, date: str) -> dict:
    logger.info(f"Starting extract func")

    tado_data = read_extracted_historic(metadata)
    zone_data = read_extracted_zone(metadata)

    logger.debug(tado_data.keys())

    keys = tado_data.keys()
    zone_id = metadata["zone_id"]
    base_path = Path(metadata["base_path"])
    days_path = generate_save_path(
        base_path, zone_id, date, ext=".csv", suffix="_days"
    )
    climate_path = generate_save_path(
        base_path, zone_id, date, ext=".csv", suffix="_climate"
    )

    # EXTRACT FROM TADO_DATA
    settings = pd.json_normalize(
        tado_data["settings"],
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )
    other = (
        pd.DataFrame(
            {
                k: [v]
                for k, v in tado_data.items()
                if k in ["hoursInDay", "zoneType", "interval"]
            }
        )
        .assign(
            from_=lambda x: x.interval.apply(lambda y: y["from"]),
            to=lambda x: x.interval.apply(lambda y: y["to"]),
        )
        .rename(columns={"from_": "from"})
        .drop(columns=["interval"])
    )

    humidity = pd.json_normalize(
        tado_data["measuredData"]["humidity"],
        "dataPoints",
        ["max", "min", "percentageUnit", "valueType"],
        meta_prefix="_",
    )
    interior_temp = pd.json_normalize(
        tado_data["measuredData"]["insideTemperature"],
        "dataPoints",
        [["max", "celsius"], ["min", "celsius"], "valueType"],
        meta_prefix="_",
    )
    measuring_device_connected = pd.json_normalize(
        tado_data["measuredData"]["measuringDeviceConnected"],
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    stripes = pd.json_normalize(
        tado_data["stripes"],
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )
    call_for_heat = pd.json_normalize(
        tado_data["callForHeat"], "dataIntervals", ["timeSeriesType", "valueType"]
    )

    weather_condition = pd.json_normalize(
        tado_data["weather"]["condition"],
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )
    weather_slots = (
        pd.DataFrame.from_dict(tado_data["weather"]["slots"]["slots"], orient="index")
        .assign(temperature=lambda x: x["temperature"].apply(lambda y: y["celsius"]))
        .rename(columns={"temperature": "temperature_celsius"})
    )
    weather_sunny = pd.json_normalize(
        tado_data["weather"]["sunny"],
        "dataIntervals",
        ["timeSeriesType", "valueType"],
        meta_prefix="_",
    )

    # TRANSFORM STEP 1
    # CREATE 'days' record
    def generate_days(other: pd.DataFrame) -> pd.DataFrame:
        days = other.assign(
            day_id=UUID,
            extracted_path=str(base_path),
            transformed_path=str(climate_path),
            date=date,
        )
        return days

    days = generate_days(other)
    days.to_csv(days_path, index=False)

    def generate_climate(
        humidity: pd.DataFrame, interior_temp: pd.DataFrame
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
                day_id=UUID,
                extracted_path=str(base_path),
                transformed_path=str(climate_path),
                date=date,
            )
        )
        return climate

    climate = generate_climate(humidity, interior_temp)
    climate.to_csv(climate_path, index=False)

    logger.debug(days)

    # EXTRACT FROM ZONE_DATA

    logger.debug('Updating metadata')
    metadata_new = metadata.copy()
    metadata_new["transformed"] = {
        "climate_path": climate_path,
        "days_path": days_path,
    }
    return metadata_updated
