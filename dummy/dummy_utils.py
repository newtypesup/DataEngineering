import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
dotenv_path = os.path.join(base_path, '.env')
load_dotenv(dotenv_path=dotenv_path)

def db_conn():
  username = os.getenv("DB_USER")
  password = os.getenv("DB_PASSWORD")
  host = os.getenv("DB_HOST")
  port = os.getenv("DB_PORT")
  database = os.getenv("DB_NAME")
  engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
  return engine


def get_input_table():
  input_table = os.getenv("INPUT_TABLE")
  query = (f"SELECT * FROM {input_table};")
  return query

def get_ccd_table():
  ccd_table = os.getenv("CCD_TABLE")
  ccd_table_list = [value.strip() for value in ccd_table.split(",")]
  query = [f"SELECT * FROM {table};" for table in ccd_table_list]
  return query


def load_df(engine, query):
  return pd.read_sql_query(text(query), engine)