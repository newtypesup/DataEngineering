import boto3 #type: ignore
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore
from datetime import datetime

KST = pendulum.timezone("Asia/Seoul")

def weekly_cate_top5_job(job_name):
    glue = boto3.client('glue', region_name='ap-northeast-2')
    response = glue.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Start Glue JobName: {job_name}, JobRunId: {job_run_id}")

with DAG(
    dag_id='weekly_cate_top5',
    start_date=datetime(2025, 5, 5, tzinfo=KST),
    schedule='5 9 * * 1',  # 매주 월요일 오전 9시 5분 KST
    catchup=False,
    tags=['weekly', 'category', 'top5'],
) as dag:

    weekly_cate_top5 = PythonOperator(
        task_id='weekly_cate_top5',
        python_callable=weekly_cate_top5_job,
        op_args=['weekly_cate_top5'],
    )

    weekly_cate_top5