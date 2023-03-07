import logging

import pendulum
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BASE_PATH = "/opt/airflow/dags/files"


@dag(
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=True,
    tags=["etl", "tado", "smart-home", "IoT"],
    max_active_runs=1,
)
def tado_etl_dag():
    """
    ### ETL pipeline for Tado API

    For ingesting data from domestic smart heating system into a Postgres DB

    - Extracts data from API using PyTado 3rd party api wrapper
    - Validates unstructured data (json) data against Pydantic data model
    - Transforms into tabular data structure
    - Loads to Postgres DB via SQLAlchemy
    """

    extract = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_extract_task",
        task_id="extract_task",
        command="python -m tasks.cli.extract",
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
        tty=True,
    )

    validate = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_validate_task",
        task_id="validate_task",
        command="python -m tasks.cli.validate",
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
        tty=True,
    )

    transform = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_transform_task",
        task_id="transform_task",
        command="python -m tasks.cli.transform",
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
        tty=True,
    )

    load = DockerOperator(
        image="docker_image_task",
        container_name="docker_container_load_task",
        task_id="load_task",
        command="python -m tasks.cli.load",
        # command='apt update && apt install iputils-ping && ping database -c 3',
        # command="ls",
        mounts=[
            Mount(
                source="/workspaces/house_climate/dags",
                target="/opt/airflow/dags",
                type="bind",
            ),
        ],
        environment={
            "XCOM_PULL": "{{ ti.xcom_pull('transform_task') }}",
        },
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,
        network_mode="house_climate_default",
    )

    # DAG
    extract >> validate >> transform >> load


tado_etl_dag()
