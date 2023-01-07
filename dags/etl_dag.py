import json
import logging
from pathlib import Path
from typing import Union

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl"],
)
def etl_dag():
    """
    ### ETL pipeline for tado api into postgres
    """

    @task(multiple_outputs=True)
    def extract_task(date: str):
        """
        #### Extract data using Tado API

        Extracts json response from Tado API, saves to disk.

        Todo list:
        - Add upload of raw data to S3 bucket
        - Do I need some logic for duplicate detection
        - Further validation checks (or should this be a separate airflow task?)
        """
        from tasks.extract import extract

        path = "/opt/airflow/dags/files"

        logger.debug("EXTRACT: execution date: %s", date)
        result, metadata = extract(path, date)
        logger.debug("EXTRACT: finished")
        return {"result": result, "metadata": metadata}

    @task
    def transform_task(extracted, date: str, metadata: dict):
        """#### Transform the data"""
        from tasks.transform import transform

        logger.debug("TRANSFORM: execution date: %s", date)
        transformed = transform(extracted, date, metadata)
        logger.debug("TRANSFORM: finished")
        return transformed

    @task
    def load_task(transformed, date: str):
        """#### Print to logger stream"""
        logger.debug("LOAD: execution date: %s", date)
        print(transformed)

    # Define the graph
    extracted = extract_task("{{ ds }}")
    transformed = transform_task(extracted["result"], "{{ ds }}", extracted["metadata"])
    load_task(transformed, "{{ ds }}")


etl_dag()
