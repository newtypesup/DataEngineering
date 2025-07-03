# import os
# base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..'))
# sys.path.append(base_path)
import pandas as pd
from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# dotenv_path = os.path.join(base_path, '.env')
# load_dotenv(dotenv_path=dotenv_path)

DB_HOST='localhost'
DB_PORT=5434
DB_USER='fastapi'
DB_PASSWORD=12345
DB_NAME='fastapi'
INPUT_TABLE='df_input'
CCD_TABLE=['df_category','df_content','df_download']

def db_conn():
  username = DB_USER
  password = DB_PASSWORD
  host = DB_HOST
  port = DB_PORT
  database = DB_NAME
  engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
  print('1')
  return engine
# def db_conn():
#   username = os.getenv("DB_USER")
#   password = os.getenv("DB_PASSWORD")
#   host = os.getenv("DB_HOST")
#   port = os.getenv("DB_PORT")
#   database = os.getenv("DB_NAME")
#   engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
#   print('1')
#   return engine


def get_input_table():
  input_table = INPUT_TABLE
  query = (f"SELECT * FROM {input_table};")
  print('2')
  return query


def get_ccd_table():
  ccd_table = CCD_TABLE
  ccd_table_list = [value.strip() for value in ccd_table.split(",")]
  query = [f"SELECT * FROM {table};" for table in ccd_table_list]
  print("3")
  return query


def load_df(engine, query):
  print('4')
  return pd.read_sql_query(text(query), engine)