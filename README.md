# house_climate
Using Tado API to analyse data from smart thermostat + TRVs etc.

## High-level summary
- Developed an ETL pipeline using Airflow.
    - Incrementally extract data from Tado API and load to PostGres DB.
    - Configured Airflow through docker compose.
    - Data validation using Pydantic.
- Plan to aggregate heating system metrics over the full 2022 year's data.
- Probably going to figure out some way to dashboard the results.
- May deploy to AWS etc. Currently running airflow locally.
- May look into anomaly detection to highlight unusual days.
    - May require comparison with weather data (which is already in the Tado data).
