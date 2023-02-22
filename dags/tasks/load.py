import json
import logging
from pathlib import Path
from typing import Union

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tasks.helpers.common import read_text_file
from tasks.helpers.data_models import Metadata

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SQL_QUERIES_DIR = Path("tasks/sql_queries")
CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@database:5432/postgres"


def load(metadata: Metadata) -> Metadata:
    # Upload to postgres raw table
    logger.debug("Creating connection to DB")
    engine = create_engine(CONNECTION_STRING, echo=True)
    logger.debug("Connection to DB created")

    # Load records from csv files
    days_records = load_records_from_csv(metadata.transform.days_all_path)
    interior_records = load_records_from_csv(metadata.transform.interior_all_path)
    weather_records = load_records_from_csv(metadata.transform.weather_all_path)
    call_for_heat_records = load_records_from_csv(
        metadata.transform.call_for_heat_all_path
    )

    # Create table if not exists
    create_tables_query = read_text_file(SQL_QUERIES_DIR / "create_tables.sql")
    create_tables(engine, create_tables_query)

    # Perform UPSERT into table
    # Read .sql queries and load records to db
    insert_days_query = read_text_file(SQL_QUERIES_DIR / "insert_days.sql")
    load_rows_to_db(engine, days_records, insert_days_query)

    insert_interior_query = read_text_file(SQL_QUERIES_DIR / "insert_interior.sql")
    load_rows_to_db(engine, interior_records, insert_interior_query)

    insert_weather_query = read_text_file(SQL_QUERIES_DIR / "insert_weather.sql")
    load_rows_to_db(engine, weather_records, insert_weather_query)

    insert_call_for_heat_query = read_text_file(
        SQL_QUERIES_DIR / "insert_call_for_heat.sql"
    )
    load_rows_to_db(engine, call_for_heat_records, insert_call_for_heat_query)

    logger.debug("Updating metadata")
    metadata_new = metadata.copy()
    return metadata_new


def create_tables(engine, query) -> None:
    """First create the new tables in postgres"""
    with engine.begin() as conn:
        tables_before = conn.execute(
            text(
                """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public';
            """
            )
        )
        logger.debug("Tables in DB before: %s", tables_before)

        logger.debug("Creating tables")
        conn.execute(text(query))
        logger.debug("Created new tables")

        tables_after = conn.execute(
            text(
                """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public';
            """
            )
        )
        logger.debug("Tables in DB after: %s", tables_after)


def load_rows_to_db(engine, records: list[dict], query: str):
    """Inserts list of dict records to a db. based on .execute_many()"""
    logger.debug("Starting load_data_generic")
    with engine.begin() as conn:
        conn.execute(
            text(query),
            records,
        )
    logger.debug("Completed load_data_generic")


def load_records_from_csv(path: str) -> list[dict]:
    """Prepare a list of dict records by loading rows from a .csv"""
    records = pd.read_csv(path, index_col=None).to_dict(orient="records")
    return records
