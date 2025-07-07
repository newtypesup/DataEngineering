import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timezone
from scripts.Create_dummy_data import create_dummy
from scripts.Uploading_dummy_parquet_to_s3 import to_parquet_and_upload

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="Create_dummy_data_dag",
    schedule_interval="* */2 * * *", # 매 2시간마다
    start_date=datetime(2025, 7, 1, 0, 0, tzinfo=KST),
    end_date=datetime(2025, 7, 31, 0, 0, tzinfo=KST),
    catchup=False,
    tags=['create', 'dummy', 'upload', 'parquet', 's3']) as dag:
    create_dummy_data = PythonOperator(
                                    task_id='create_dummy',
                                    python_callable=create_dummy,
                                    dag=dag                            
                                )
    upload_to_s3 = PythonOperator(
                                    task_id="upload_to_s3",
                                    python_callable=to_parquet_and_upload,
                                    dag=dag
                                )
        
    create_dummy_data >> upload_to_s3