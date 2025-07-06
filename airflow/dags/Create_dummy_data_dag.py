import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timezone
from scripts.Create_dummy_data import create_dummy
from scripts.Uploading_dummy_parquet_to_s3 import to_parquet_and_upload

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="Create_dummy_data_dag",
    schedule_interval="*/2 * * * *", # glue 테스트를 위한 사전 데이터 배치 코드
    # schedule_interval="* */2 * * *", # 기존 코드
    #schedule_interval = "@hourly",
    start_date=datetime(2025, 7, 6, 22, 26, tzinfo=KST),
    end_date=datetime(2025, 7, 19, 0, 0, tzinfo=KST),
    catchup=False,
    tags=['create', 'dummy', 'upload', 'parquet', 's3']) as dag:
    create_dummy_data = PythonOperator(
                                    task_id='create_dummy',
                                    python_callable=create_dummy                                  
                                )
    upload_to_s3 = PythonOperator(
                                    task_id="upload_to_s3",
                                    python_callable=to_parquet_and_upload,
                                    provide_context=True
                                )
        
    create_dummy_data >> upload_to_s3