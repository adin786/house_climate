import json
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tasks.helpers.data_models import Metadata

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_zone_data(engine, metadata: Metadata):
    with engine.begin() as conn:
        logger.debug("Creating table")
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
        logger.debug("Created new table")

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


def load(metadata: Metadata):

    # Upload to postgres raw table
    logger.debug("Creating connection to DB")
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@database:5432/postgres", echo=True
    )
    logger.debug("Connection to DB created")

    # with engine.begin() as conn:
    #     logger.debug('Dropping table')
    #     conn.execute(text('DROP TABLE IF EXISTS raw_data;'))
    #     logger.debug('Dropped existing table')

    # TODO: Load up all csv files from disk

    # TODO: Create table if not exists, define schema

    # TODO: Perform UPSERT into table

    # TODO: Delete files from disk, maybe in a bash script

    load_zone_data()

    logger.debug("Updating metadata")
    metadata_new = metadata.copy()
    return metadata_new
