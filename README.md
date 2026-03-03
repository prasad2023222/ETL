# ETL Weather Project

This project contains an Apache Airflow DAG that extracts current weather data from the Open-Meteo API and loads it into a Postgres database.

## Project Structure

- `dags/etlweather.py` – Airflow DAG that:
  - Extracts current weather data for a given latitude/longitude.
  - Transforms the JSON response.
  - Loads the data into a `weather_data` table in Postgres.
- `airflow_settings.yaml` – Local Airflow connections, variables, and pools.
- `Dockerfile` – Defines the Airflow image for running this project in Docker.
- `.env` – Environment variables used by the project (not committed to Git).
- `.gitignore` – Git ignore rules for this project.

## Prerequisites

- Docker and Docker Compose installed.
- Astronomer CLI (if you are using `astro dev` to run Airflow).
- Git installed and configured with your GitHub account.

## Running Airflow Locally

If this project was created with Astronomer, you can start Airflow locally with:

astro dev start
