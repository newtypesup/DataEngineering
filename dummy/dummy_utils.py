#####dummy_utils.py
from sqlalchemy import create_engine

username = "fastapi"
password = "12345"
host = "localhost"
port = 5434
database = "fastapi"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}")
query = "SELECT * FROM df_input;"