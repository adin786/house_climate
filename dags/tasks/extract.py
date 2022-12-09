import os
from PyTado.interface import Tado
from dotenv import load_dotenv
import logging
from pathlib import Path
from typing import Union, Optional
import json
import pendulum


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
SELECTED_ZONE = 'Downstairs hallway'


class MissingZone:
    """Raise if no matching zone found"""


def extract(
    path: Union[str, Path], 
    date: str
) -> tuple[dict, dict]:
    logger.info(f'Starting extract func')

    t = Tado(os.environ['TADO_USERNAME'], os.environ['TADO_PASSWORD'])
    logger.debug(f'API connected')

    # Request list of zone IDs
    zones = t.getZones()
    logger.debug(f'Zones:\n{[z["name"] for z in zones]}')
    try:
        zone_id = [z['id'] for z in zones if z['name'] == SELECTED_ZONE][0]
    except IndexError:
        raise MissingZone('No living room zone')
        
    # API request for 24hrs data
    tado_data = t.getHistoric(zone_id, date=date)
    logger.debug(f'living_room_data:\n{list(tado_data.keys())}')
    validate_raw_data(tado_data)

    # Make target file path
    # save_path = Path(path) / f'tado_zone{zone_id}_{date}.json'
    save_path = generate_save_path(path, zone_id, date, suffix='', ext='.json')
    save_path = Path(save_path)
    if save_path.is_file():
        raise FileExistsError(f"File already exists at {str(save_path)}. Exiting")

    # Write file
    logger.debug(f'Saving to disk ({str(save_path)})')
    json_data = json.dumps(tado_data, sort_keys=True, indent=4)
    save_path.write_text(json_data, encoding='utf-8')
    logger.debug('Saving completed')

    metadata = {
        'extracted_path': save_path,
        'zone_id': zone_id,
        'date': date,
    }
    return tado_data, metadata


def generate_save_path(
    base_path: Union[str, Path], 
    zone_id: str, 
    date: str, 
    ext: str, 
    suffix: Optional[str] = None
) -> str:
    save_path = (Path(base_path) / f'tado_zone{zone_id}_{date}{suffix}').with_suffix(ext)
    return str(save_path)


def validate_raw_data(tado_data: dict):
    """Run some checks on the tado response"""
    logger.info('Running tado validation ')
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
        logger.error('Tado data failed validation checks')
        raise err
    logger.info('Tado validation passed')
