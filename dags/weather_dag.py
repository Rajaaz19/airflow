from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os

# File path
FILE_PATH = "/opt/airflow/dags/weather_data.csv"


def fetch_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast?latitude=13.0827&longitude=80.2707&current_weather=true"
    response = requests.get(url)
    data = response.json()

    context["ti"].xcom_push(key="weather_data", value=data["current_weather"])


def save_to_csv(**context):
    weather = context["ti"].xcom_pull(key="weather_data", task_ids="fetch_weather_task")

    with open(FILE_PATH, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["temperature", "windspeed", "time"])
        writer.writerow([
            weather["temperature"],
            weather["windspeed"],
            weather["time"]
        ])


def print_success():
    print("Weather data pipeline completed successfully!")


with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["weather", "api", "data_pipeline"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather_task",
        python_callable=fetch_weather
    )

    save_task = PythonOperator(
        task_id="save_to_csv_task",
        python_callable=save_to_csv
    )

    success_task = PythonOperator(
        task_id="print_success_task",
        python_callable=print_success
    )

    fetch_task >> save_task >> success_task