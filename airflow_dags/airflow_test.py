from airflow import DAG
from airflow.operators.bash import BashOperator
<<<<<<< HEAD
from datetime import datetime, timezone, timedelta


KST = timezone(timedelta(hours=9))

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 6, 8, 20, 50, tzinfo=KST),  # 한국 시간
    schedule="*/10 * * * *",
    catchup=False
) as dag:
=======
from datetime import datetime

with DAG(dag_id="hello_world", start_date=datetime(2025, 1, 1), schedule="@daily", catchup=False) as dag:
>>>>>>> e487343951ad49bacd0d0c1284ff748a5a2f5595
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo Hello from Airflow!"
    )
<<<<<<< HEAD
=======
#test
>>>>>>> e487343951ad49bacd0d0c1284ff748a5a2f5595
