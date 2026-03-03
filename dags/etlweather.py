
from airflow import DAG
from airflow.sdk import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

Latitude="40.4637"
Longitude="3.7492"
POSTGRES_CONN_ID="postgres_default"
API_CONN_ID="open_meteo_api"

default_args={
    "owner":"airflow",
    "start_date":datetime(2026,1,1)
}

with DAG(dag_id="etl_weather",default_args=default_args,schedule="@daily",catchup=False)as dags:
    @task()
    def extract_weather_data():
        http_hook=HttpHook(http_conn_id=API_CONN_ID,method="GET")

        endpoint=f'/v1/forecast?latitude={Latitude}&longitude={Longitude}&current_weather=true'

        response=http_hook.run(endpoint)

        if response.status_code==200:
            return response.json()

        else:
            raise Exception(f"failed tp extract weather data:{response.status_code}")


    @task()
    def transform_weather_data(weather_data):
        current_weather=weather_data["current_weather"]
        transformed_data={
            "latitude":Latitude,
            "longitude":Longitude,
            "temperature":current_weather["temperature"],
            "windspeed":current_weather["windspeed"],
            "winddirection":current_weather["winddirection"],
            "weathercode":current_weather["weathercode"]

        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        pg_hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn=pg_hook.get_conn()
        cursor=conn.cursor()
        cursor.execute(""" create table if not exists weather_data(
        latitude float,
        longitude float,
        temperature float,
        windspeed float,
        winddirection float,
        weathercode int,
        timestamp timestamp);""")
       
        cursor.execute(" insert into weather_data(latitude,longitude,temperature,windspeed,winddirection,weathercode)values(%s,%s,%s,%s,%s,%s)",(
        transformed_data["latitude"],
        transformed_data["longitude"],
        transformed_data["temperature"],
        transformed_data["windspeed"],
        transformed_data["winddirection"],
        transformed_data["weathercode"]
       ))

        conn.commit()
        cursor.close()


    weather_data=extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)