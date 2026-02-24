from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone


def say_hello():
    """Prints hello world message."""
    print("Hello, World!")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


with DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="A simple Hello World DAG example",
    schedule=None,
    start_date=timezone.datetime(2026, 2, 21),
    catchup=False,
    tags=["example", "hello-world"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello
    )

    end = EmptyOperator(task_id="end")

    start >> hello_task >> end