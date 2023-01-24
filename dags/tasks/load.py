import json
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tasks.helpers.data_models import Metadata
from tasks.helpers.common import read_text_file
from typing import Union
from pathlib import Path

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SQL_QUERIES_DIR = Path('dags/tasks/sql_queries')
CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@database:5432/postgres"


def create_tables(engine) -> None:
    """Run this first to create new tables if needed"""
    create_table_query = read_text_file(SQL_QUERIES_DIR / 'create_tables.sql')
    with engine.begin() as conn:
        logger.debug("Creating table")
        conn.execute(text(create_table_query))
        logger.debug("Created new table")


def load_zone_data(engine, metadata: Metadata):
    """Read processed csv files and insert into SQL tables"""
    # TODO: Base my INSERT solution on https://stackoverflow.com/a/57529830

    with engine.begin() as conn:
        logger.debug("Inserting new row into table")
        conn.execute(
            text(
                """
                INSERT INTO raw_data (date, json)
                VALUES (:d, :j)
                ON CONFLICT (date)
                DO NOTHING;
                """
            ),
            {"d": date, "j": json.dumps(tado_data)},
        )
        logger.debug("Inserted new row")

        conn.execute(
            text(
                """
                INSERT INTO raw_data (date, json)
                VALUES (:d, :j)
                ON CONFLICT (date)
                DO NOTHING;
                """
            ),
            {"d": date, "j": json.dumps(tado_data)},
        )
        logger.debug("Passed duplicate insert")

    return metadata


def load(metadata: Metadata):

    # Upload to postgres raw table
    logger.debug("Creating connection to DB")
    engine = create_engine(
        CONNECTION_STRING, echo=True
    )
    logger.debug("Connection to DB created")

    # with engine.begin() as conn:
    #     logger.debug('Dropping table')
    #     conn.execute(text('DROP TABLE IF EXISTS raw_data;'))
    #     logger.debug('Dropped existing table')

    # TODO: Load up all csv files from disk

    # TODO: Create table if not exists, define schema
    create_tables(engine)

    # TODO: Perform UPSERT into table
    # RAW json table

    # TODO: Delete files from disk, maybe in a bash script

    metdata = load_zone_data(engine, metadata)

    logger.debug("Updating metadata")
    metadata_new = metadata.copy()
    return metadata_new
