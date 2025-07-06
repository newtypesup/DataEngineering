import os, sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..'))
sys.path.append(base_path)
import io
import boto3 # type: ignore
import pendulum
from dummy.dummy_utils import db_conn, get_ccd_table, load_df

def to_parquet_and_upload(**context):
    # 기존 코드
    # TIME = pendulum.now("Asia/Seoul").strftime("%Y%m%d%H")
    
    KST = pendulum.timezone("Asia/Seoul") # glue 테스트를 위한 사전 데이터 배치 코드 🔻
    base_time = pendulum.datetime(2025, 7, 1, 10, 0, tz=KST)
    start_time = pendulum.datetime(2025, 7, 6, 22, 26, tz=KST)
    execution_date = context['execution_date']
    # pendulum 객체로 변환 (혹시 모를 타입 문제 방지)
    if not isinstance(execution_date, pendulum.DateTime):
        execution_date = pendulum.instance(execution_date).in_timezone(KST)
    else:
        execution_date = execution_date.in_timezone(KST)
    elapsed_minutes = (execution_date - start_time).in_minutes()
    execution_count = elapsed_minutes // 2
    hours_to_add = execution_count * 2
    target_time = base_time.add(hours=hours_to_add)
    TIME = target_time.strftime("%Y%m%d%H") # glue 테스트를 위한 사전 데이터 배치 코드 🔺
    
    LIST = ['category', 'content', 'download']
    BUCKET_NAME = 'newtypesup'
    engine = db_conn()
    query = get_ccd_table()

    s3_client = boto3.client('s3')
    results = []
    for i in range(len(query)):
        df = load_df(engine, query[i])
        s3_key = f"etl/raw/{LIST[i]}/{TIME[2:4]}/{TIME[4:6]}/{TIME[6:8]}/{LIST[i]}_{TIME}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="gzip")
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, BUCKET_NAME, s3_key)
        results.append(f"s3://{BUCKET_NAME}/{s3_key}")
    return results

if __name__ == "__main__":
  try:
    results = to_parquet_and_upload()
    for file in results:
      print(file)
  except Exception as ex:
    print(ex)
    print("=" * 50)
    print("failed")
    print("=" * 50)
  finally:
    print("=" * 50)
    print("success")
    print("=" * 50)
