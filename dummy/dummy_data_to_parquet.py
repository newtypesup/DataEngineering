##### 로컬 테스트 #####
import os
import pandas as pd
from datetime import datetime
from dummy_utils import db_conn, get_ccd_table, load_df

TIME = datetime.now().strftime("%Y%m%d%H")
LIST = ['category', 'content', 'download']

engine = db_conn()
ccd = get_ccd_table()

def to_parquet():
  for _ in range(len(ccd)):
    df = load_df(engine, ccd[_])
    output_dir = f"C:/Py/DE/Parquet_test/{LIST[_]}/{TIME[2:4]}/{TIME[4:6]}/{TIME[6:8]}/".format(LIST, TIME)
    output_file = f"{LIST[_]}_{TIME}.parquet".format(LIST, TIME)
    full_path = os.path.join(output_dir, output_file)
    os.makedirs(output_dir, exist_ok = True)
    df.to_parquet(full_path, engine = "pyarrow", compression = "gzip")
    yield output_file

if __name__ == "__main__":
  try:
    output_file = to_parquet()
    for file in output_file:
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
