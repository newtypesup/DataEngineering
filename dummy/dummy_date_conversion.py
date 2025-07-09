import random
import pandas as pd
from time import time
from datetime import datetime, timedelta
from dummy_utils import db_conn, get_input_table, load_df
import sqlalchemy

engine = db_conn()
query = get_input_table()
df_input = load_df(engine, query)

def _reg_date_conversion(df_input):
    start_date = datetime.strptime('2020-01-01', "%Y-%m-%d").date()
    end_date = datetime.strptime('2025-05-01', "%Y-%m-%d").date()
    df_input['reg_date'] = [
        (start_date + timedelta(days=random.randint(0, (end_date - start_date).days))).strftime("%Y-%m-%d")
        for _ in range(len(df_input))
    ]
    return df_input

if __name__ == "__main__":
    start_time = time()
    try:
        df_input = _reg_date_conversion(df_input)
        # 각 row의 ctnt_id에 대해 reg_date만 UPDATE
        with engine.begin() as conn:
            for idx, row in df_input.iterrows():
                conn.execute(
                    sqlalchemy.text("UPDATE df_input SET reg_date = :reg_date WHERE ctnt_id = :ctnt_id"),
                    {"reg_date": row['reg_date'], "ctnt_id": row['ctnt_id']}
                )
        print(df_input.head())
        print(len(df_input), "개")
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

    