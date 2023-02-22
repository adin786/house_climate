# Tado API Analysis Project
Using Tado API to analyse data from smart thermostat + TRVs etc.

## High-level summary
- Developed an ETL pipeline using `Airflow`.
    - Extract from `PyTado` API using `backoff` to handle retries
    - Incremental (daily) load to `PostGres` DB.
    - Configured Airflow through `docker compose`.
    - JSON schema validation using `Pydantic`.
    - Data transform using `Pandas`.
    - Load to DB using `SQLAlchemy`.
    - **(Future)** May build in data validation using Great `Expectations` or `Pandera`. 
- Plan to aggregate heating system metrics over the full 2022 year's data.
- Probably going to figure out some way to dashboard the results, Dash, Grafana etc.
- May deploy to AWS etc. Currently running Airflow locally.
- May look into anomaly detection to highlight unusual heating days.
    - May require comparison with weather data (which is already in the Tado API data).

---

# Prerequisites
- Docker is installed
- VSCode is installed with Dev Containers ext. (for development only)
- ...

# Docker containers

All elements of this project were designed to be run inside docker containers. My development environment is using a VSCode devcontainer defined in [devcontainer.json](.devcontainer/devcontainer.json). Should be possible to use this with Github Codespaces too.

My `docker-compose.yaml` is a customised version of the [airflow template](https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml) and configures all the airflow services (scheduler, webserver etc) in addition to my Postgres local database (I might move this to an RDS instance later).

# Data pipeline - *ETL, airflow*

I used Airflow for orchestration and wrote a DAG which breaks up the extract, transform, load steps into discrete operations with clear dependencies.

Airflow should be used as an orchestrator and not as an execution engine, so all my tasks are built using `DockerOperator` to isolate my Python code's dependencies from airflow's python environment.

![ETL steps in DAG](docs/images/etl_steps.excalidraw.png)


# How to run the airflow DAG yourself
A `Makefile` is provided with several helpful commands for spinning up the airflow services on your machine.

## Build the custom worker image
In order to use the DockerOperator in my ETL DAG, we need to build a Docker image tagged "docker_image_task".  Use the command:

```bash
make containers_build
```

## Init the Airflow backend DB
Using docker compose we can spin up the first (of several) Airflow related containers with the command:

```bash
make containers_init
```

## Spin up all containers to run Airflow
Using docker compose again we can now spin up the other Airflow containers such as webserver, scheduler etc using the command:

```bash
make containers_up
```

At this point, the Airflow web UI should become available on http://localhost:8080

## Spin down all containers
Shut down Airflow and stop the containers

```bash
make containers_down
```