#schemas 정의
from pydantic import BaseModel
from typing import Literal


class ContentInput(BaseModel):
  ctnt_name: str
  cate_name: Literal['게임', '도구', '여행', '소셜 미디어', '음악', '맞춤 설정', '오피스', '사진', '글꼴']
  age_ratings: Literal['전체이용가', '12세이용가', '15세이용가', '18세이용가']
