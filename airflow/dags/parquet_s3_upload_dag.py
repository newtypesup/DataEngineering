import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
from scripts.parquet_s3_upload_script import to_parquet_and_upload

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="parquet_s3_upload_dag",
    schedule_interval="0 */2 * * *",
    #schedule_interval = "@hourly",
    start_date=datetime(2025, 7, 1, tzinfo=KST),
    catchup=False,
    tags=['upload', 'parquet', 's3']) as dag:
        upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=to_parquet_and_upload
    )
        #upload_to_s3



