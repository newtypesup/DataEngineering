##### dummy_data_date.py
import random
import pandas as pd
from time import time
from faker import Faker
from datetime import datetime, timedelta
from dummy_utils import engine, query

fake = Faker()
Faker.seed(0)

df_input = pd.read_sql_query(query, engine)

def _reg_date_conversion(engine):
    
    # 시작 날짜는 datetime 객체로 변환
    start_date = datetime.strptime('2020-01-01', "%Y-%m-%d").date()
    end_date = datetime.now().date()
    df_input['reg_date'] = [(start_date + timedelta(days = random.randint(0, (end_date - start_date).days))).strftime("%Y-%m-%d") for _ in range(len(df_input))]

    with engine.connect() as conn:
        df_input.to_sql('df_input', conn, if_exists='replace', index=False, chunksize=100)
        
    return conn
    
if __name__ == "__main__":
    try:
        x = time()
        conn = _reg_date_conversion(engine)
        print(df_input.head())
        conn.close()
        print("=" * 50)
        print("success")
        print("=" * 50)
        print("소요시간 :", round(time() - x, 2))

    except Exception as ex:
        print(ex)
        conn.close()
        print("=" * 50)
        print("fail")
        print("=" * 50)
    