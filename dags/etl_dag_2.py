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

BASE_PATH = "/opt/airflow/dags/files"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl"],
)
def etl_dag_2():
    """
    ### ETL pipeline for tado api into postgres
    """

    # docker_task = DockerOperator(
    #     image="docker_image_task",
    #     container_name="docker_container_dummy_task",
    #     mounts=[
    #         Mount(source='/workspaces/house_climate/dags', target='/opt/airflow/dags', type='bind'),
    #     ],
    #     environment = {
    #         "LOGICAL_DATE": "{{ ds }}"
    #     },
    #     auto_remove=True,
    #     mount_tmp_dir=False,
    #     task_id="dummy_task",
    #     docker_url='unix://var/run/docker.sock',
    #     command='echo \'{"Hello": "World"}\'',
    # )

    extract_task = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_extract_task",
        mounts=[
            Mount(
                source="/workspaces/house_climate/dags",
                target="/opt/airflow/dags",
                type="bind",
            ),
        ],
        environment={
            "BASE_PATH": BASE_PATH,
            "LOGICAL_DATE": "{{ ds }}",
            "TADO_USERNAME": "{{ var.value.TADO_USERNAME }}",
            "TADO_PASSWORD": "{{ var.value.TADO_PASSWORD }}",
        },
        auto_remove=True,
        mount_tmp_dir=False,
        task_id="extract_task",
        tty=True,
        command="python -m tasks.cli.extract",
    )

    validate_task = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_validate_task",
        mounts=[
            Mount(
                source="/workspaces/house_climate/dags",
                target="/opt/airflow/dags",
                type="bind",
            ),
        ],
        environment={
            "XCOM_PULL": "{{ ti.xcom_pull('extract_task') }}",
        },
        auto_remove=True,
        mount_tmp_dir=False,
        task_id="validate_task",
        tty=True,
        command="python -m tasks.cli.validate",
    )

    transform_task = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_transform_task",
        mounts=[
            Mount(
                source="/workspaces/house_climate/dags",
                target="/opt/airflow/dags",
                type="bind",
            ),
        ],
        environment={
            "XCOM_PULL": "{{ ti.xcom_pull('validate_task') }}",
        },
        auto_remove=True,
        mount_tmp_dir=False,
        task_id="transform_task",
        tty=True,
        command="python -m tasks.cli.transform",
    )

    load_task = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_load_task",
        mounts=[
            Mount(
                source="/workspaces/house_climate/dags",
                target="/opt/airflow/dags",
                type="bind",
            ),
        ],
        environment={
            "BASE_PATH": BASE_PATH,
            "LOGICAL_DATE": "{{ ds }}",
            "XCOM_PULL": "{{ ti.xcom_pull('transform_task') }}",
            "TADO_USERNAME": "{{ var.value.TADO_USERNAME }}",
            "TADO_PASSWORD": "{{ var.value.TADO_PASSWORD }}",
        },
        auto_remove=True,
        mount_tmp_dir=False,
        task_id="load_task",
        tty=True,
        command="python -m tasks.cli.load",
    )

    # DAG
    extract_task >> validate_task >> transform_task >> load_task


etl_dag_2()
