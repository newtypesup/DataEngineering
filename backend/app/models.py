# sqlalchemy 모델 정의
from sqlalchemy import Column, BigInteger, Text, Date, UniqueConstraint
from app.database import Base
from datetime import datetime


class Content(Base):
  __tablename__ = "df_input"
  __table_args__ = (UniqueConstraint("ctnt_name",
                                     "cate_name",
                                     "age_ratings",
                                     name = "unique_content"), )
  ctnt_id = Column(BigInteger, primary_key = True, index = True)
  ctnt_name = Column(Text, nullable = False)
  cate_name = Column(Text, nullable = False)
  age_ratings = Column(Text, nullable = False)
  reg_date = Column(Date, default = datetime.now().date())
