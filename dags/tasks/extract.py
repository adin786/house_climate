import json
import logging
import os
import uuid
from pathlib import Path
from typing import Union, Optional
import shutil

import pandas as pd
import pendulum
from dotenv import load_dotenv
from PyTado.interface import Tado
from sqlalchemy import create_engine, text
from tasks.common import generate_save_path


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
SELECTED_ZONE = "Downstairs hallway"
UUID = uuid.uuid4().hex
EXTRACTED_ON = pendulum.now()


class MissingZone:
    """Raise if no matching zone found"""


def validate_raw_data(tado_data: dict):
    """Run some checks on the tado response"""
    logger.info("Running tado validation ")
    try:
        assert isinstance(tado_data, dict)

        assert "hoursInDay" in tado_data
        assert tado_data["hoursInDay"] == 24

        assert "interval" in tado_data
        start_date = pendulum.parse(tado_data["interval"]["from"])
        end_date = pendulum.parse(tado_data["interval"]["to"])
        delta = end_date.diff(start_date)
        logger.debug(f"{start_date=}")
        logger.debug(f"{end_date=}")
        logger.debug(f"Data duration {delta.in_hours()} H ({delta.in_minutes()} min)")
        assert delta.in_hours() >= 24

    except AssertionError as err:
        logger.error("Tado data failed validation checks")
        raise err
    logger.info("Tado validation passed")


def extract_zone_data(t: Tado, zones: list, zone_id: str, date: str) -> dict:
    """Extracts one zone from Tado API
    
    Args:
        t (Tado): Connected tado API session object.
        zones (list): list of zone metadata dicts from getZones request.
        zone_id (int): 

    Returns:
        dict: Response from getHistoric request.
    """
    zone_name = get_zone_name(zones, zone_id)
    logger.debug('Extracting zone id: %s, name: %s', zone_id, zone_name)
    # Get API response for 24hrs data
    tado_data = t.getHistoric(zone_id, date=date)
    logger.debug(f"living_room_data:\n{list(tado_data.keys())}")
    validate_raw_data(tado_data)
    return tado_data


def get_zone_name(zones: list, zone_id: int) -> str:
    """Get zone_name from zone_id"""
    zone_names = [z["name"] for z in zones if z["id"] == zone_id]
    if len(zone_names) == 0:
        raise MissingZone(f'Did not find zone name for zone_id: {zone_id}')
    elif len(zone_names) > 1:
        raise MissingZone(f'More than one zone name for zone_id: {zone_id} -> {zone_names}')
    return zone_names[0]


def clear_files_in_dir(path: Union[str, Path]):
    path = Path(path)
    files_in_dir = [p.name for p in path.glob('*')]
    num_files = len(files_in_dir)
    logger.info('Deleting (%s) files in dir: %s', num_files, files_in_dir)
    shutil.rmtree(path)
    logger.debug('Files deleted')


def save_historic_data(tado_data: dict, path: str, date: str, zone_id: int) -> None:
    """Save result to disk"""
    # Make target file path
    historic_path = Path(generate_save_path(path, zone_id, date, suffix="_historic", ext=".json"))
    # Write file
    logger.debug("Saving zone_id: %s to path: %s", zone_id, str(historic_path))
    json_data = json.dumps(tado_data, sort_keys=True, indent=4)
    historic_path.write_text(json_data, encoding="utf-8")
    logger.debug("Saving completed")
    return str(historic_path)


def save_zone_data(zones: list, path: Union[str, Path], date: str) -> str:
    # Make target file path
    zones_path = Path(generate_save_path(
        path=path, 
        zone_id="_all", 
        date=date, 
        suffix="_zones", 
        ext=".json"
    ))
    logger.debug("Saving zone metadata to path: %s", zones_path)
    json_data = json.dumps(zones, sort_keys=True, indent=4)
    zones_path.write_text(json_data, encoding="utf-8")
    logger.debug("Saving completed")
    return str(zones_path)


def extract(path: Union[str, Path], date: str) -> tuple[dict, dict]:
    """Extracts all available zone data from API and saves to .json"""
    logger.info(f"Starting extract func")

    logger.info('Connecting to API')
    t = Tado(os.environ["TADO_USERNAME"], os.environ["TADO_PASSWORD"])
    logger.debug(f"API connected")

    # Request list of zone IDs
    logger.info('Getting zone metadata')
    zones = t.getZones()
    zone_names = [z["name"] for z in zones]
    zone_ids = [z["id"] for z in zones]
    logger.debug('Zones: %s', list(zip(zone_names, zone_ids)))

    # Save zone data to disk
    extracted_zone_data = save_zone_data(zones, path, date)

    # Extract and save all zones, one by one
    extracted_historic_data = []
    for zone_id in zone_ids:
        tado_data = extract_zone_data(t, zones, zone_id, date)

        historic_path = save_historic_data(tado_data, path, date, zone_id)
        extracted_historic_data.append(historic_path)

    # Output metadata for next task
    metadata = {
        "base_path": str(path),
        "extract": {
            "historic_data": extracted_historic_data,
            "one_data": extracted_zone_data,
        },
        "date": date,
    }
    return tado_data, metadata


