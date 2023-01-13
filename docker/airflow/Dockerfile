# FROM apache/airflow:2.5.0
FROM apache/airflow:2.5.0-python3.9

ARG base_path=docker/airflow

USER root
RUN apt-get update
RUN apt-get install -y git
USER airflow


COPY ${base_path}/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
