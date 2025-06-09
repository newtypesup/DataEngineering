from sqlalchemy import create_engine

db_url = "postgresql+psycopg2://test_user:12345@localhost:5432/test_db"
engine = create_engine(db_url)





