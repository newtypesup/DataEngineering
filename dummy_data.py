from faker import Faker
import random
import pandas as pd

# Faker 설정
fake = Faker('ko_KR')
Faker.seed(0)

# 데이터 개수 설정
count = 1000000

# 카테고리 정보 (category.csv)
category = {
    '100000': ['000000', '교육', '12세'],
    '100001': ['100000', '아동', '7세'],
    '100002': ['100000', '어학', '12세'],
    '200000': ['000000', '게임', '12세'],
    '200001': ['000000', '게임', '15세'],
    '300000': ['000000', '유틸', '12세']
}

# 콘텐츠 정보 (content.csv)
content = {
    '100000': ['듀오링고', '말해보카', '스픽'],
    '100001': ['모바일펜스', '베베베모', '마미톡'],
    '100002': ['밀리의서재', '예스24', '교보문고'],
    '200000': ['포켓몬고', '배틀그라운드', '로블록스'],
    '200001': ['배틀그라운드', '던파', '메이플'],
    '300000': ['업비트', '키움증권', '기업은행']
}

# 1. category.csv 데이터 생성
cate_ids = [random.choice(list(category.keys())) for _ in range(count)]
parent_ids = [category[c][0] for c in cate_ids]
cate_names = [category[c][1] for c in cate_ids]
age_ratings = [category[c][2] for c in cate_ids]

df_category = pd.DataFrame({
    'cate_id': cate_ids,
    'parent_id': parent_ids,
    'cate_name': cate_names,
    'age_rating': age_ratings
})
df_category.to_csv('C:\\Py\\ETL\\category.csv', index=False, encoding="utf-8-sig")

# 2. content.csv 데이터 생성 (cate_id 동일)
ctnt_ids = [f'{i:07d}' for i in range(1, count + 1)]
ctnt_names = [random.choice(content[c]) for c in cate_ids]  # cate_id에 해당하는 콘텐츠 랜덤 선택
reg_dates = [fake.past_date() for _ in range(count)]

df_content = pd.DataFrame({
    'ctnt_id': ctnt_ids,
    'cate_id': cate_ids,  # category.csv와 FK 관계 유지
    'ctnt_name': ctnt_names,
    'reg_date': reg_dates
})
df_content.to_csv('C:\\Py\\ETL\\content.csv', index=False, encoding="utf-8-sig")

# 3. download.csv 데이터 생성 (ctnt_id 동일)
cnty_codes = [random.choices(['KOR', 'US', 'CHN', 'JP'], weights=[0.7, 0.1, 0.1, 0.1])[0] for _ in range(count)]
statuses = ['success'] * count
dates = [fake.past_date() for _ in range(count)]

df_download = pd.DataFrame({
    'ctnt_id': ctnt_ids,  # content.csv와 FK 관계 유지
    'cnty_cd': cnty_codes,
    'status': statuses,
    'date': dates
})
df_download.to_csv('C:\\Py\\ETL\\download.csv', index=False, encoding="utf-8-sig")

print("CSV 파일 생성 완료!")