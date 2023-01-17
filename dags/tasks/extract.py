import json
import logging
import os
import uuid
from pathlib import Path
from typing import Union, Optional
import shutil

# import pandas as pd
import pendulum
from dotenv import load_dotenv
from PyTado.interface import Tado
# from sqlalchemy import create_engine, text
from tasks.helpers.common import generate_save_path
from tasks.helpers.data_models import Metadata, ExtractField, HistoricDataItem
from tasks.helpers.tado_data_models import TadoDataModel


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
SELECTED_ZONE = "Downstairs hallway"
UUID = uuid.uuid4().hex
EXTRACTED_ON = pendulum.now()


class MissingZone:
    """Raise if no matching zone found"""


def extract_zone_data(t: Tado, zones: list, zone_id: str, date: str) -> TadoDataModel:
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

    # Parse with Pydantic for validatio of JSON schema + 24 hours check
    tado_data = TadoDataModel(**tado_data)
    logger.debug(f'Extraction done for zone: {zone_id}')
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
    paths = list(path.glob('*'))
    files = [p.name for p in paths]
    num_files = len(files)
    logger.info('Deleting %s files in dir: %s', num_files, files)
    # shutil.rmtree(path)
    for p in paths:
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p)
        else:
            raise NotImplementedError()
    logger.debug('Files deleted')


def save_historic_data(tado_data: TadoDataModel, path: str, date: str, zone_id: int) -> None:
    """Save result to disk"""
    # Make target file path
    historic_path = Path(generate_save_path(path, zone_id, date, suffix="_historic", ext=".json"))

    # Write file
    logger.debug("Saving zone_id: %s to path: %s", zone_id, str(historic_path))
    json_data = tado_data.json(by_alias=True, sort_keys=True, indent=4)
    historic_path.write_text(json_data, encoding="utf-8")
    logger.debug("Saving completed")
    return str(historic_path)


def save_zone_data(zones: list, path: Union[str, Path], date: str) -> str:
    # Make target file path
    zones_path = Path(generate_save_path(
        base_path=path, 
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


def extract(metadata: Metadata) -> Metadata:
    """Extracts all available zone data from API and saves to .json
    Returns a metadata dict to xcom for next task"""
    logger.info("Starting extract func")
    path = Path(metadata.base_path)
    date = metadata.date

    logger.info('Deleting `files/` folder')
    clear_files_in_dir(path)

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
        extracted_historic_data.append(
            HistoricDataItem(path=historic_path, zone_id=zone_id)
        )

    # Output metadata for next task
    logger.debug('Updating metadata')
    metadata.extract = ExtractField(
        historic_data=extracted_historic_data,
        zone_data=extracted_zone_data,
    )
    return metadata


