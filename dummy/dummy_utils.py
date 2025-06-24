import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def db_conn():
  username = os.getenv("DB_USER")
  password = os.getenv("DB_PASSWORD")
  host = os.getenv("DB_HOST")
  port = os.getenv("DB_PORT")
  database = os.getenv("DB_NAME")
  return create_engine(
      f"postgresql://{username}:{password}@{host}:{port}/{database}")


def get_input_table():
  input_table = os.getenv("INPUT_TABLE")
  return (f"SELECT * FROM {input_table};")


def load_df(engine, query):
  return pd.read_sql_query(query, engine)
