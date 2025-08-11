import os, sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..'))
sys.path.append(base_path)
import random
import pendulum
import pandas as pd
from time import time
from datetime import datetime, timedelta, timezone
from sqlalchemy.types import BigInteger, Integer, String, Date
from dummy.dummy_utils import db_conn, get_input_table, load_df

utc = datetime.now(timezone.utc)
def create_dummy(**context):

    engine = db_conn()
    query = get_input_table()
    df_input = load_df(engine, query)

    # 결과를 누적할 리스트
    category_rows = []
    content_rows = []
    download_rows = []
    users_regions_map = {}

    KST = pendulum.timezone("Asia/Seoul") # glue 테스트를 위한 사전 데이터 배치 코드 🔻
    base_time = pendulum.datetime(2025, 5, 1, 0, 0, tz=KST) # 기준이 될 최초 날짜 
    start_time = pendulum.datetime(2025, 8, 1, 0, 0, tz=KST) # dag의 첫 실행 날짜
    execution_date = context['execution_date']

    if not isinstance(execution_date, pendulum.DateTime):
        execution_date = pendulum.instance(execution_date).in_timezone(KST)
    else:
        execution_date = execution_date.in_timezone(KST)
    elapsed_minutes = (execution_date - start_time).in_minutes()
    execution_count = elapsed_minutes // 2
    hours_to_add = execution_count * 2
    target_time = base_time.add(hours=hours_to_add)
    print("타겟 날짜 target_time", target_time)# glue 테스트를 위한 사전 데이터 배치 코드 🔺

    # 반복 처리
    for idx, row in df_input.iterrows():
        # 1. 변칙률 계산
        count = round(random.uniform(0.7, 1.3), 2)
        reg_date = pd.to_datetime(row['reg_date'])
        date_diff = (target_time.date() - reg_date.date()).days + 10 # glue 테스트를 위한 사전 데이터 배치 코드
        print("날짜 값 date_diff", date_diff)
        num = count * (date_diff**0.75)
        spike = random.random()
        if spike < 0.05:
            multiplier = random.uniform(0.2, 1.8)
        elif spike < 0.20:
            multiplier = random.uniform(0.7, 1.3)
        else:
            multiplier = 1.0
        final = int(round(num * multiplier))

        # 2. cate_id 및 parent_id 분기 처리
        if row['cate_name'] == '게임':
            if row['age_ratings'] == '전체이용가':
                cate_id = '10000'
            elif row['age_ratings'] == '12세이용가':
                cate_id = '10012'
            elif row['age_ratings'] == '15세이용가':
                cate_id = '10015'
            else:
                cate_id = '10018'
            parent_id = '0'

        elif row['cate_name'] == '도구':
            cate_id = '20000'
            parent_id = '1'
        elif row['cate_name'] == '여행':
            cate_id = '30000'
            parent_id = '1'
        elif row['cate_name'] == '소셜 미디어':
            cate_id = '40000'
            parent_id = '1'
        elif row['cate_name'] == '음악':
            cate_id = '50000'
            parent_id = '1'
        elif row['cate_name'] == '맟춤 설정':
            cate_id = '60000'
            parent_id = '1'
        elif row['cate_name'] == '오피스':
            cate_id = '70000'
            parent_id = '1'
        elif row['cate_name'] == '사진':
            cate_id = '80000'
            parent_id = '1'
        else:
            cate_id = '90000'
            parent_id = '1'

        # 3. num 만큼 데이터 생성
        for _ in range(final):
            create_uid = "abcdefghijklmnopqrstuvwxyz1234567890"
            digit = random.randint(7, 7)
            user_id = "".join(random.sample(create_uid, digit))

            if user_id in users_regions_map:
                region = users_regions_map[user_id]
            else:
                region = random.choices(['KOR', 'USA', 'JPN', 'CHN', 'etc'],
                                        weights=[0.3, 0.2, 0.1, 0.2, 0.2])[0]
                users_regions_map[user_id] = region

            status = random.choices(['SUCCESS', 'FAIL'], weights=[0.9, 0.1])[0]

            # category
            category_rows.append({
                'cate_id': cate_id,
                'parent_id': parent_id,
                'cate_name': row['cate_name'],
                'age_ratings': row['age_ratings'],
                'uid': user_id,
                'run_time': target_time.strftime("%Y%m%d%H") # glue 테스트를 위한 사전 데이터 배치 코드
            })

            # content
            content_rows.append({
                'ctnt_id': row['ctnt_id'],
                'cate_id': cate_id,
                'ctnt_name': row['ctnt_name'],
                'reg_date': row['reg_date'],
                'uid': user_id,
                'run_time': target_time.strftime("%Y%m%d%H") # glue 테스트를 위한 사전 데이터 배치 코드
            })

            # download
            download_rows.append({
                'ctnt_id': row['ctnt_id'],
                'cnty_cd': region,
                'status': status,
                'date': target_time.date(), # glue 테스트를 위한 사전 데이터 배치 코드
                'uid': user_id,
                'run_time': target_time.strftime("%Y%m%d%H") # glue 테스트를 위한 사전 데이터 배치 코드
            })

    # 4. 최종 DataFrame 생성
    df_category = pd.DataFrame(category_rows).sort_values('parent_id')
    df_content = pd.DataFrame(content_rows)
    df_download = pd.DataFrame(download_rows)

    category_dtypes = {
                        'cate_id' : BigInteger(),
                        'parent_id' : Integer(),
                        'cate_name' : String(50),
                        'age_ratings' : String(50),
                        'uid' : String(50),
                        'run_time' : String(50)
                    }
    content_dtypes = {
                        'ctnt_id' : Integer(),
                        'cate_id' : BigInteger(),
                        'ctnt_name' : String(50),
                        'reg_date' : String(50),
                        'uid' : String(50),
                        'run_time' : String(50)
                    }
    download_dtypes = {
                        'ctnt_id' : Integer(),
                        'cnty_cd' : String(50),
                        'status' : String(50),
                        'date' : Date(),
                        'uid' : String(50),
                        'run_time' : String(50)
                    }

    df_category.to_sql('df_category',
                        engine,
                        if_exists='replace',
                        index=False,
                        chunksize=1000,
                        dtype=category_dtypes
                    )
    df_content.to_sql('df_content',
                        engine,
                        if_exists='replace',
                        index=False,
                        chunksize=1000,
                        dtype=content_dtypes
                    )
    df_download.to_sql('df_download',
                        engine,
                        if_exists='replace',
                        index=False,
                        chunksize=1000,
                        dtype=download_dtypes
                    )
    
    return df_category, df_content, df_download


if __name__ == "__main__":
    start_time = time()
    try:
        df_category, df_content, df_download = create_dummy()
        print(df_category.head())
        print(len(df_category), "개")
        print(df_content.head())
        print(len(df_content), "개")
        print(df_download.head())
        print(len(df_download), "개")
        print("=" * 50)
        print("success")
        print("=" * 50)
    except Exception as ex:
        print(ex)
        print("=" * 50)
        print("failed")
        print("=" * 50)
    finally:
        print("소요시간 :", round(time() - start_time, 2), "초")
