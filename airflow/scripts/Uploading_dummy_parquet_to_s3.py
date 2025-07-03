import os, sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..'))
sys.path.append(base_path)
import io
import boto3
import pendulum
from dummy.dummy_utils import db_conn, get_ccd_table, load_df

def to_parquet_and_upload():
    TIME = pendulum.now("Asia/Seoul").strftime("%Y%m%d%H")
    LIST = ['category', 'content', 'download']
    BUCKET_NAME = 'newtypesup'
    engine = db_conn()
    query = get_ccd_table()

    s3_client = boto3.client('s3')

    for i in range(len(query)):
        results = []
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
