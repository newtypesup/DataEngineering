import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timezone, timedelta

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="hello_world",
    start_date=pendulum.datetime(2025, 6, 1, 1, 1, tz="Asia/Seoul"),
    schedule="*/10 * * * *",
    catchup=False
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo Hello from Airflow!"
    )
