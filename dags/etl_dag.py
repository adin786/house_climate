
import json
import pendulum
from airflow.decorators import dag, task
import logging
from typing import Union
from pathlib import Path


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


# # Exit with warning if not virtualenv available
# if not shutil.which("virtualenv"):
#     log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl"],
)
def etl_dag():
    """
    ### Test DAG
    """

    @task(multiple_outputs=True)
    def extract_task(date: str):
        """
        #### Extract data using Tado API

        Currently extracts json response from Tado API, saves to disk.

        Todo list:
        - Add upload of raw data to S3 bucket
        - Do I need some logic for duplicate detection
        - Further validation checks (or should this be a separate airflow task?)
        """
        from tasks.extract import extract

        path = '/opt/airflow/dags/files'

        log.debug(f'EXTRACT: execution date: {date}')
        result, metadata = extract(path, date)
        log.debug(f'EXTRACT: finished')
        return {'result': result, 'metadata': metadata}


    @task
    def transform_task(extracted, date: str, metadata: dict):
        """#### Transform the data"""
        from tasks.transform import transform

        log.debug(f'TRANSFORM: execution date: {date}')
        transformed = transform(extracted, date, metadata)
        log.debug(f'TRANSFORM: finished')
        return transformed


    @task
    def load_task(transformed, date: str):
        """#### Print to log stream"""
        log.debug(f'LOAD: execution date: {date}')
        print(transformed)


    # Define the graph
    extracted = extract_task('{{ ds }}')
    transformed = transform_task(extracted['result'], '{{ ds }}', extracted['metadata'])
    load_task(transformed, '{{ ds }}')

    # @task(multiple_outputs=True)
    # def transform(order_data_dict: dict):
    #     """
    #     #### Transform task
    #     A simple Transform task which takes in the collection of order data and
    #     computes the total order value.
    #     """
    #     total_order_value = 0
    #
    #     for value in order_data_dict.values():
    #         total_order_value += value
    #
    #     return {"total_order_value": total_order_value}
    #
    #
    # @task()
    # def load(total_order_value: float):
    #     """
    #     #### Load task
    #     A simple Load task which takes in the result of the Transform task and
    #     instead of saving it to end user review, just prints it out.
    #     """
    #
    #     print(f"Total order value is: {total_order_value:.2f}")
    #
    #
    # order_data = extract()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])


etl_dag()
