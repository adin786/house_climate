import json
import logging
from pathlib import Path
from typing import Union

import pendulum
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl"],
)
def etl_dag():
    """
    ### ETL pipeline for tado api into postgres
    """

    docker_task = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_task0",
        mounts=[
            Mount(source='/workspaces/house_climate/dags', target='/opt/airflow/dags', type='bind'),
        ],
        auto_remove=True,
        mount_tmp_dir=False,
        task_id="docker_task_test",
        docker_url='unix://var/run/docker.sock',
        command="ls",
    )

    @task.docker(
        image="docker_image_task",
        container_name='docker_container_task',
        mounts=[
            Mount(source='/workspaces/house_climate/dags', target='/opt/airflow/dags', type='bind'),
            # Mount(source='./dags', target='/opt/airflow/dags', type='bind'),
        ],
        auto_remove=True,
        mount_tmp_dir=False,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
    )
    def extract_task(date: str):
        """
        #### Extract data using Tado API

        Extracts json response from Tado API, saves to disk.

        Todo list:
        - Add upload of raw data to S3 bucket
        - Do I need some logic for duplicate detection
        - Further validation checks (or should this be a separate airflow task?)
        """
        import os
        import logging

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.debug('CURRENT DIR: %s', os.listdir())
        print(f'CURRENT DIR: {os.listdir()}')
        from tasks.extract import extract

        path = "/opt/airflow/dags/files"

        logger.debug("EXTRACT: execution date: %s", date)
        metadata = extract(path, date)
        logger.debug("EXTRACT: finished")
        return metadata


    @task
    def transform_task(metadata, date: str):
        """#### Transform the data"""
        from tasks.transform import transform

        logger.debug("TRANSFORM: execution date: %s", date)
        metadata_new = transform(metadata, date)
        logger.debug("TRANSFORM: finished")
        return metadata_new


    @task
    def load_task(transformed, date: str):
        """#### Print to logger stream"""
        logger.debug("LOAD: execution date: %s", date)
        print(transformed)

    # Define the graph
    metadata = extract_task("{{ ds }}")
    docker_task >> metadata
    transformed = transform_task(metadata, "{{ ds }}")
    load_task(transformed, "{{ ds }}")


etl_dag()
