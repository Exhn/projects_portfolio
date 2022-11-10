"""Python script for quering DataBase for universidad del salvador"""
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# Get the path to the .env file
envFile = Path(__file__).parent.parent.parent / "src" / "py_functions" / "vars.env"

# Loading variables from dotenv
load_dotenv(envFile)

postgres_conn_id = os.getenv('postgres_conn_id')
path = Path(__file__).parent.parent.parent / "src" / "sql" / "Universidad del Salvador.sql"
csvPath = Path(__file__).parent.parent.parent / "destFolder" / "Universidad del Salvador.csv"


def querysalvador():
    """Python function to download query from directory and querying
    the Postgres database for Universidad del Salvador"""
    engine = create_engine(postgres_conn_id)
    query = open(path, 'r')
    result = pd.read_sql(query.read(), engine)
    result.to_csv(csvPath, index=False)
