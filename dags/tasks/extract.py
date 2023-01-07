import json
import logging
import os
import uuid
from pathlib import Path
from typing import Union

import pandas as pd
import pendulum
from dotenv import load_dotenv
from PyTado.interface import Tado
from sqlalchemy import create_engine, text
from tasks.common import generate_save_path, validate_raw_data


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
SELECTED_ZONE = "Downstairs hallway"
UUID = uuid.uuid4().hex
EXTRACTED_ON = pendulum.now()


class MissingZone:
    """Raise if no matching zone found"""


def extract(path: Union[str, Path], date: str) -> tuple[dict, dict]:
    logger.info(f"Starting extract func")

    t = Tado(os.environ["TADO_USERNAME"], os.environ["TADO_PASSWORD"])
    logger.debug(f"API connected")

    # Request list of zone IDs
    zones = t.getZones()
    logger.debug(f'Zones:\n{[z["name"] for z in zones]}')
    try:
        zone_id = [z["id"] for z in zones if z["name"] == SELECTED_ZONE][0]
    except IndexError:
        raise MissingZone("No living room zone")

    # Get API response for 24hrs data
    tado_data = t.getHistoric(zone_id, date=date)
    logger.debug(f"living_room_data:\n{list(tado_data.keys())}")
    validate_raw_data(tado_data)

    # Make target file path
    save_path = generate_save_path(path, zone_id, date, suffix="", ext=".json")
    save_path = Path(save_path)
    if save_path.is_file():
        # Delete if already existing file on disk
        save_path.unlink()

    # Write file
    logger.debug(f"Saving to disk ({str(save_path)})")
    json_data = json.dumps(tado_data, sort_keys=True, indent=4)
    save_path.write_text(json_data, encoding="utf-8")
    logger.debug("Saving completed")

    # Upload to postgres raw table
    logger.debug('Creating connection to DB')
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@database:5432/postgres", echo=True
    )
    logger.debug('Connection to DB created')

    # with engine.begin() as conn:
    #     logger.debug('Dropping table')
    #     conn.execute(text('DROP TABLE IF EXISTS raw_data;'))
    #     logger.debug('Dropped existing table')

    with engine.begin() as conn:
        logger.debug('Creating table')
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS raw_data (
                    date date UNIQUE NOT NULL,
                    json json NOT NULL
                );
                """
            )
        )
        logger.debug('Created new table')

    with engine.begin() as conn:
        logger.debug('Inserting new row into table')
        conn.execute(
            text(
                """
                INSERT INTO raw_data (date, json)
                VALUES (:d, :j)
                ON CONFLICT (date)
                DO NOTHING;
                """
            ),
            {
                "d": date, 
                "j": json.dumps(tado_data)
            }   
        )
        logger.debug('Inserted new row')

        conn.execute(
            text(
                """
                INSERT INTO raw_data (date, json)
                VALUES (:d, :j)
                ON CONFLICT (date)
                DO NOTHING;
                """
            ),
            {
                "d": date, 
                "j": json.dumps(tado_data)
            }   
        )
        logger.debug('Passed duplicate insert')


    metadata = {
        "extracted_path": str(save_path),
        "zone_id": zone_id,
        "date": date,
    }
    return tado_data, metadata


