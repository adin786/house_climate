FROM python:3.9.13

# For jupyter server
EXPOSE 8888

ARG base_path=docker/airflow_docker_operator

# Dev tools
RUN apt-get update
RUN apt-get install build-essential -y
    
# Dev tools
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install \
    pip==22.3.1    

# ==========================
# ==== Project specific ====
# ==========================

# Project python dependencies
WORKDIR /tmp
COPY ${base_path}/requirements.txt requirements.txt
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install -r requirements.txt
