import pendulum
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from scripts.Create_dummy_data import create_dummy
from scripts.Uploading_dummy_parquet_to_s3 import to_parquet_and_upload
from utils.Slack_alert import slack_fail_alert, slack_success_alert

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'on_failure_callback': slack_fail_alert,
}
with DAG(
    dag_id="Create_dummy_data",
    default_args=default_args,
    schedule_interval="* */2 * * *", # 매 2시간마다
    start_date=datetime(2025, 5, 1, 0, 0, tzinfo=KST),
    # end_date=datetime(2025, 8, 1, 0, 0, tzinfo=KST),
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
                                    on_success_callback=slack_success_alert,
                                    dag=dag
                                )
        
    create_dummy_data >> upload_to_s3