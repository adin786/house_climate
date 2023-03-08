import json
import logging
from pathlib import Path
from typing import Union

import pandas as pd
from tasks.helpers.common import generate_save_path, get_zone_item_by_id, read_text_file
from tasks.helpers.data_models import Metadata, TransformedZoneItem, TransformField
from tasks.helpers.tado_data_models import TadoDataModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def transform(metadata: Metadata) -> Metadata:
    logger.info(f"Starting transform func")

    metadata.transform = TransformField(zones=[])
    for i in range(len(metadata.validate_.zones)):

        # Transform historic data for all and save to csv. Adds path to metadata
        metadata = transform_historic_data(metadata, i)

    # Transform zone info table and save to csv. Adds path to metadata
    metadata = transform_zones_data(metadata)

    # Transform to load in and concatenate all historic tables per zone
    metadata = transform_merge_historic(metadata)

    logger.debug("Finished transform func")
    logger.debug("metadata %s", metadata)
    return metadata


def lookup_zone_metadata_by_id(path: Union[str, Path], zone_id: int) -> dict:
    zones_metadata_text = Path(path).read_text(encoding="utf-8")
    zones_metadata = json.loads(zones_metadata_text)
    zones_lookup = {e["id"]: e for e in zones_metadata}
    return zones_lookup[zone_id]


def generate_days(
    metadata: Metadata, other_fields: pd.DataFrame, zone_id: int
) -> pd.DataFrame:
    zone_metadata = lookup_zone_metadata_by_id(metadata.validate_.zones_path, zone_id)
    this_zone = get_zone_item_by_id(metadata.validate_.zones, zone_id)
    historic_data = json.loads(read_text_file(this_zone.path))

    # Generate days df
    days = other_fields.rename(
        columns={
            "hoursInDay": "hours_in_day",
            "zoneType": "zone_type",
            "from": "t_start",
            "to": "t_end",
        }
    ).assign(
        zone_id=zone_id,
        zone_name=zone_metadata["name"],
        extracted_date=metadata.date,
        historic_data=json.dumps(historic_data),
        zone_metadata=json.dumps(zone_metadata),
    )
    return days


def generate_interior(
    metadata: Metadata,
    humidity: pd.DataFrame,
    interior_temp: pd.DataFrame,
    zone_id: int,
) -> pd.DataFrame:

    if len(humidity) == 0:
        logger.debug("Zone %s humidity table empty", zone_id)
        humidity = pd.DataFrame(
            columns=[
                "_max",
                "_min",
                "_percentageUnit",
                "_valueType",
                "timestamp",
                "value",
            ]
        )

    if len(interior_temp) == 0:
        logger.debug("Zone %s interior_temp table empty", zone_id)
        interior_temp = pd.DataFrame(
            columns=[
                "_max.celsius",
                "_max.fahrenheit",
                "_min.celsius",
                "_min.fahrenheit",
                "_valueType",
                "timestamp",
                "value.celsius",
                "value.fahrenheit",
            ]
        )

    interior = (
        pd.merge(left=humidity, right=interior_temp, on="timestamp", how="outer")
        .assign(
            temperature_unit="celsius",
            zone_id=zone_id,
            extracted_date=metadata.date,
        )
        .rename(
            columns={
                "_valueType_x": "humidity_unit",
                "value.celsius": "temperature",
                "value": "humidity",
            }
        )
        .loc[
            :,
            [
                "extracted_date",
                "timestamp",
                "zone_id",
                "humidity",
                "humidity_unit",
                "temperature",
                "temperature_unit",
            ],
        ]
    )
    return interior


def generate_weather(
    metadata: Metadata, weather_condition: pd.DataFrame, zone_id: int
) -> pd.DataFrame:
    """Applying transformation to"""
    weather = (
        weather_condition.assign(zone_id=zone_id, extracted_date=metadata.date)
        .drop(columns=["value.temperature.fahrenheit", "_timeSeriesType", "_valueType"])
        .rename(
            columns={
                "value.state": "state",
                "value.temperature.celsius": "temp_celsius",
                "from": "t_start",
                "to": "t_end",
            }
        )
        .loc[
            :,
            [
                "extracted_date",
                "t_start",
                "t_end",
                "zone_id",
                "state",
                "temp_celsius",
            ],
        ]
    )
    return weather


def generate_call_for_heat(
    metadata: Metadata, call_for_heat: pd.DataFrame, zone_id: int
) -> pd.DataFrame:

    # Generate days df
    call_for_heat = (
        call_for_heat.assign(zone_id=zone_id, extracted_date=metadata.date)
        .rename(
            columns={
                "from": "t_start",
                "to": "t_end",
            }
        )
        .loc[
            :,
            [
                "zone_id",
                "extracted_date",
                "t_start",
                "t_end",
                "value",
            ],
        ]
    )
    return call_for_heat


def read_extracted_zone(metadata: Metadata) -> list:
    """Read zone metadatadata file into a list"""
    # TODO: probably refactor with pydantic
    path = Path(metadata.validate_.zones_path)
    return json.loads(path.read_text(encoding="utf-8"))


def transform_historic_data(metadata: Metadata, i):
    """Transform one zone's worth of JSON data into csv tables"""
    path = metadata.validate_.zones[i].path
    zone_id = metadata.validate_.zones[i].zone_id
    logger.info("BEGIN transform for zone_id: %s", zone_id)
    tado_data = TadoDataModel.parse_file(path)
    logger.debug("JSON data parsed to TadoDataModel")

    days_path = generate_save_path(
        metadata,
        zone_id,
        ext=".csv",
        suffix="_days",
    )
    interior_path = generate_save_path(
        metadata,
        zone_id,
        ext=".csv",
        suffix="_interior",
    )
    weather_path = generate_save_path(
        metadata,
        zone_id,
        ext=".csv",
        suffix="_weather",
    )
    call_for_heat_path = generate_save_path(
        metadata,
        zone_id,
        ext=".csv",
        suffix="_call_for_heat",
    )
    metadata.transform.zones.append(
        TransformedZoneItem(
            interior_path=interior_path,
            days_path=days_path,
            weather_path=weather_path,
            call_for_heat_path=call_for_heat_path,
            zone_id=zone_id,
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

    # # UNUSED
    # measuring_device_connected = pd.json_normalize(
    #     tado_data.measuredData.measuringDeviceConnected.dict(by_alias=True),
    #     "dataIntervals",
    #     ["timeSeriesType", "valueType"],
    #     meta_prefix="_",
    # )

    # # UNUSED
    # # TODO: Maybe to get added in to weather metadata table?
    # stripes = pd.json_normalize(
    #     tado_data.stripes.dict(by_alias=True),
    #     "dataIntervals",
    #     ["timeSeriesType", "valueType"],
    #     meta_prefix="_",
    # )

    # TODO: Maybe useful in future
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

    # # TODO: Maybe useful in future
    # weather_slots = (
    #     pd.DataFrame.from_dict(tado_data.weather.slots.slots, orient="index")
    #     .assign(
    #         temperature=lambda x: x["temperature"].apply(lambda y: y["celsius"])
    #     )
    #     .rename(columns={"temperature": "temperature_celsius"})
    # )

    # # UNUSED
    # weather_sunny = pd.json_normalize(
    #     tado_data.weather.sunny.dict(by_alias=True),
    #     "dataIntervals",
    #     ["timeSeriesType", "valueType"],
    #     meta_prefix="_",
    # )

    logger.debug(
        "Preprocessed basic tables, transform into days, climate and weather tables"
    )

    # TRANSFORM STEP 1
    # CREATE 'days' rows
    days = generate_days(metadata, other_fields, zone_id)
    days.to_csv(days_path, index=False)

    # TRANSFORM STEP 2
    # CREATE 'interior' rows
    interior = generate_interior(metadata, humidity, interior_temp, zone_id)
    interior.to_csv(interior_path, index=False)

    # TRANSFORM STEP 3
    # CREATE 'weather' rows
    weather = generate_weather(metadata, weather_condition, zone_id)
    weather.to_csv(weather_path, index=False)

    # TRANSFORM STEP 4
    # CREATE 'call_for_heat' rows
    call_for_heat = generate_call_for_heat(metadata, call_for_heat, zone_id)
    call_for_heat.to_csv(call_for_heat_path, index=False)

    logger.debug("days shape: %s", days.shape)
    logger.debug("interior shape: %s", interior.shape)
    logger.debug("weather shape: %s", weather.shape)

    return metadata


def transform_zones_data(metadata: Metadata) -> Metadata:
    zones_path = generate_save_path(
        metadata,
        "_all",
        ext=".csv",
        suffix="_zones",
    )
    metadata.transform.zones_path = zones_path
    zone_list = read_extracted_zone(metadata)
    zone_data = pd.DataFrame(zone_list)
    zone_data.to_csv(zones_path, index=False)
    return metadata


def transform_merge_historic(metadata: Metadata) -> Metadata:
    """Concatenate all per-zone historic data tables ready for final load step"""
    logger.debug('Concatenating each zones "interior" table')
    interior_list = []
    columns = []
    for zone_item in metadata.transform.zones:
        logger.debug("zone_id: %s", zone_item.zone_id)
        this_interior = pd.read_csv(zone_item.interior_path, index_col=False)
        this_columns = set(this_interior.columns)
        if len(columns) == 0:
            columns = this_columns
        if not columns == this_columns:
            raise ValueError(
                f"Mismatching columns during merge, expected {columns}, got {set(this_interior.columns)}"
            )
        interior_list.append(this_interior)
    interior_all = pd.concat(interior_list, axis=0)
    interior_all_path = generate_save_path(
        metadata,
        "_all",
        ext=".csv",
        suffix="_interior_all",
    )
    logger.debug("Saving interior_all to %s", interior_all_path.name)
    interior_all.to_csv(interior_all_path, index=False)
    metadata.transform.interior_all_path = interior_all_path

    logger.debug('Concatenating each zones "days" table')
    days_list = []
    columns = []
    for zone_item in metadata.transform.zones:
        logger.debug("zone_id: %s", zone_item.zone_id)
        this_days = pd.read_csv(zone_item.days_path, index_col=False)
        this_columns = set(this_days.columns)
        if len(columns) == 0:
            columns = this_columns
        if not columns == this_columns:
            raise ValueError(
                f"Mismatching columns during merge, expected {columns}, got {set(this_days.columns)}"
            )
        days_list.append(this_days)
    days_all = pd.concat(days_list, axis=0)
    days_all_path = generate_save_path(
        metadata,
        "_all",
        ext=".csv",
        suffix="_days_all",
    )
    logger.debug("Saving days_all to %s", days_all_path.name)
    days_all.to_csv(days_all_path, index=False)
    metadata.transform.days_all_path = days_all_path

    logger.debug('Concatenating each zones "weather" table')
    weather_list = []
    columns = []
    for zone_item in metadata.transform.zones:
        logger.debug("zone_id: %s", zone_item.zone_id)
        this_weather = pd.read_csv(zone_item.weather_path, index_col=False)
        this_columns = set(this_weather.columns)
        if len(columns) == 0:
            columns = this_columns
        if not columns == this_columns:
            raise ValueError(
                f"Mismatching columns during merge, expected {columns}, got {set(this_weather.columns)}"
            )
        weather_list.append(this_weather)
    weather_all = pd.concat(weather_list, axis=0)
    weather_all_path = generate_save_path(
        metadata,
        "_all",
        ext=".csv",
        suffix="_weather_all",
    )
    logger.debug("Saving weather_all to %s", weather_all_path.name)
    weather_all.to_csv(weather_all_path, index=False)
    metadata.transform.weather_all_path = weather_all_path

    logger.debug('Concatenating each zones "call_for_heat" table')
    cfh_list = []
    columns = []
    for zone_item in metadata.transform.zones:
        logger.debug("zone_id: %s", zone_item.zone_id)
        this_cfh = pd.read_csv(zone_item.call_for_heat_path, index_col=False)
        this_columns = set(this_cfh.columns)
        if len(columns) == 0:
            columns = this_columns
        if not columns == this_columns:
            raise ValueError(
                f"Mismatching columns during merge, expected {columns}, got {set(this_cfh.columns)}"
            )
        cfh_list.append(this_cfh)
    cfh_all = pd.concat(cfh_list, axis=0)
    cfh_all_path = generate_save_path(
        metadata,
        "_all",
        ext=".csv",
        suffix="_call_for_heat_all",
    )
    logger.debug("Saving call_for_heat_all to %s", cfh_all_path.name)
    cfh_all.to_csv(cfh_all_path, index=False)
    metadata.transform.call_for_heat_all_path = cfh_all_path

    return metadata
