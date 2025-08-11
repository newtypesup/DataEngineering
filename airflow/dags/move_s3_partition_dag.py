import re
import boto3 #type: ignore
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
}

LIST = ['category', 'content', 'download']
s3 = boto3.client('s3')
bucket = 'newtypesup'

def move_s3_files():
    for prefix_name in LIST:
        print(f'\n파일 옮기기 시작 : {prefix_name}')
        prefix = f'etl/raw/{prefix_name}/'

        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get('Contents', []):
                old_key = obj['Key']

                # 날짜 경로 추출: ex.25/06/01
                match = re.search(rf'etl/raw/{prefix_name}/(\d{{2}})/(\d{{2}})/(\d{{2}})/', old_key)
                if not match:
                    continue

                year = f'20{match.group(1)}'
                month = match.group(2)
                day = match.group(3)
                filename = old_key.split('/')[-1]

                new_key = f'etl/raw/{prefix_name}/year={year}/month={month}/day={day}/{filename}'

                # 복사
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': old_key},
                    Key=new_key
                )
                print(f'Copied {old_key} → {new_key}')

                # 삭제
                # s3.delete_object(Bucket=bucket, Key=old_key)
                # print(f'Deleted {old_key}')

    print("\n파일 파티션 변경 완료")

with DAG(
    dag_id='move_s3_to_partition_format_dag',
    default_args=default_args,
    schedule_interval=None,  # 트리거로 수동 실행
    catchup=False,
    tags=['s3', 'partition', 'glue'],
) as dag:

    move_task = PythonOperator(
        task_id='move_s3_files_to_partitioned_folders',
        python_callable=move_s3_files,
    )

    move_task
