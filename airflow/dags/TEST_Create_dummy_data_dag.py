import pendulum
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from scripts.TEST_Create_dummy_data import create_dummy
from scripts.TEST_Uploading_dummy_parquet_to_s3 import to_parquet_and_upload
from utils.Slack_alert import slack_fail_alert

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'on_failure_callback': slack_fail_alert,
}

with DAG(
    dag_id="T_Create_dummy_data_dag",
    default_args=default_args,
    schedule_interval="*/2 * * * *", # glue 테스트를 위한 사전 데이터 배치 코드 매 2분마다
    start_date=datetime(2025, 8, 1, 0, 0, tzinfo=KST),
    end_date=datetime(2025, 8, 2, 0, 0, tzinfo=KST),
    catchup=False,
    tags=['test', 'create', 'dummy', 'upload', 'parquet', 's3']) as dag:
    create_dummy_data = PythonOperator(
                                    task_id='create_dummy',
                                    python_callable=create_dummy,
                                    dag=dag,                                  
                                    provide_context=True # 실행 관련 값 넘기기
                                )
    upload_to_s3 = PythonOperator(
                                    task_id="upload_to_s3",
                                    python_callable=to_parquet_and_upload,
                                    dag=dag,
                                    provide_context=True # 실행 관련 값 넘기기
                                )
        
    create_dummy_data >> upload_to_s3