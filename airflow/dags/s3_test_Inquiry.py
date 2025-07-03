import boto3 # type: ignore
import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator # type: ignore

def list_s3_files():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket='newtypesup')
    for obj in response.get('Contents', []):
        logging.info(f"S3 FILE: {obj['Key']}")

with DAG(dag_id='s3_test_Inquiry_dag',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='list_s3_files',
        python_callable=list_s3_files
    )
