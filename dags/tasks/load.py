
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_zone_data(engine, metadata: dict, date: str):
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


def load(metadata, date: str):


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

    load_zone_data()