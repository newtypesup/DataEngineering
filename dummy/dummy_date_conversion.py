import random
import pandas as pd
from time import time
from faker import Faker
from datetime import datetime, timedelta
from dummy_utils import db_conn, get_input_table, load_df

fake = Faker()
Faker.seed(0)

engine = db_conn()
query = get_input_table()
df_input = load_df(engine, query)

def _reg_date_conversion(engine, df_input):
    start_date = datetime.strptime('2020-01-01', "%Y-%m-%d").date()
    end_date = datetime.now().date()
    df_input['reg_date'] = [(start_date + timedelta(days = random.randint(0, (end_date - start_date).days))).strftime("%Y-%m-%d") for _ in range(len(df_input))]

    with engine.connect() as conn:
        df_input.to_sql('df_input', conn, if_exists = 'replace', index = False, chunksize = 100)

if __name__ == "__main__":
  start_time = time()
  try:
    _reg_date_conversion(engine, df_input)
    print(df_input.head())
    print("=" * 50)
    print("success")
    print("=" * 50)

  except Exception as ex:
    print(ex)
    print("=" * 50)
    print("fail")
    print("=" * 50)

  finally:
    print("소요시간 :", round(time() - start_time, 2),"초")

    