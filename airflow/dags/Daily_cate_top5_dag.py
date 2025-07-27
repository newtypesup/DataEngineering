import boto3
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

KST = pendulum.timezone("Asia/Seoul")

def daily_cate_top5_job(job_name):
    glue = boto3.client('glue', region_name='ap-northeast-2')
    response = glue.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Start Glue JobName: {job_name}, JobRunId: {job_run_id}")

with DAG(
    dag_id='daily_cate_top5',
    start_date=datetime(2025, 5, 1, tzinfo=KST),
    schedule='0 9 * * 1',  # 매주 월요일 오전 9시 KST
    catchup=False,
    tags=['daily', 'category', 'top5'],
) as dag:

    daily_cate_top5 = PythonOperator(
        task_id='daily_cate_top5',
        python_callable=daily_cate_top5_job,
        op_args=['daily_cate_top5'],
    )

    daily_cate_top5